"""Per-user resource pools.

A *resource pool* caps how much aggregate compute (cores/memory/GPUs) a single user may
consume across their concurrently active jobs. This is a prerequisite for safely exposing
User Defined Tools (UDTs), where a user could otherwise submit many jobs and monopolise the
cluster.

Accounting is kept in an external **allocation store** (Valkey by default), *not* in Galaxy's
database, which is a permanent home for data and not the right place for ephemeral
allocations. The store holds, per ``(pool, user)``, a ledger of ``{job_id: allocation}`` for
the jobs TPV has admitted to the pool. Galaxy's job table is consulted read-only to discover
which ledgered jobs have reached a terminal state so their allocation can be released --
TPV never needs a job-completion callback.

The load-bearing operation is :meth:`AllocationStore.admit`, a single atomic
check-and-record: it drops finished jobs, sums the remaining committed usage and, if the
incoming job fits the budget (or the oversize allowance), records it. Because it is atomic,
two concurrent maps for the same user cannot both squeak past the budget.
"""

from __future__ import annotations

import importlib
import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, NamedTuple

from galaxy import model
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session, scoped_session

log = logging.getLogger(__name__)

# Galaxy job states that mean a job no longer holds any allocation. Anything not in this set
# (new, queued, running, resubmitted, or a job row we cannot see yet) is treated as live and
# keeps its ledger entry -- releasing only on positive evidence of completion is the
# conservative, never-overshoot choice.
TERMINAL_JOB_STATES = (
    "ok",
    "error",
    "failed",
    "deleted",
    "deleting",
    "stopped",
    "stopping",
    "paused",
    "skipped",
)

NORMAL = "normal"
OVERSIZE = "oversize"


class StoreUnavailable(Exception):
    """Raised by an :class:`AllocationStore` when its backend cannot be reached.

    The caller treats this as fail-closed: a job governed by a pool is deferred rather than
    admitted, so an outage of the store never silently bypasses enforcement.
    """


class ResourceUsage(NamedTuple):
    cores: float = 0.0
    mem: float = 0.0
    gpus: float = 0.0


class Budget(NamedTuple):
    """A pool budget. ``None`` on a dimension means unlimited for that dimension."""

    cores: float | None = None
    mem: float | None = None
    gpus: float | None = None


# ---------------------------------------------------------------------------
# Configuration models (declared under ``global.resource_pools`` in TPV config)
# ---------------------------------------------------------------------------
class OversizePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # How many over-budget jobs a user may run concurrently in this pool. 0 (the default)
    # means over-budget jobs are rejected outright -- the correct setting for UDT pools.
    max_concurrent: int = 0
    # Optional absolute ceiling; a job requesting more than this always fails, even within
    # the oversize allowance.
    hard_max_cores: float | None = None
    hard_max_mem: float | None = None
    hard_max_gpus: float | None = None
    # When true, a pool holds either normal jobs or a single oversize job, never both.
    reserve_pool: bool = False


class Pool(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_cores: float | None = None
    max_mem: float | None = None
    max_gpus: float | None = None
    oversize: OversizePolicy = OversizePolicy()


class ResourcePoolConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    store: str = "tpv.core.resource_pool.ValkeyAllocationStore"
    store_options: dict[str, Any] = {}
    budget_provider: str = "tpv.core.resource_pool.ConfigBudgetProvider"
    pools: dict[str, Pool] = {}


def _load_class(dotted_path: str) -> Any:
    module_path, _, name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"Not a dotted class path: {dotted_path!r}")
    return getattr(importlib.import_module(module_path), name)


# ---------------------------------------------------------------------------
# Budget providers
# ---------------------------------------------------------------------------
class BudgetProvider(ABC):
    """Resolves the :class:`Budget` for a ``(user, pool)``. Pluggable so deployments can vary
    budgets by user/role/group or an external service without changing the helper."""

    @abstractmethod
    def budget_for(self, user: Any, pool_name: str) -> Budget: ...


class ConfigBudgetProvider(BudgetProvider):
    """Default provider: one flat budget per pool from the TPV config, for every user."""

    def __init__(self, config: ResourcePoolConfig):
        self.config = config

    def budget_for(self, user: Any, pool_name: str) -> Budget:
        pool = self.config.pools.get(pool_name)
        if pool is None:
            return Budget()
        return Budget(cores=pool.max_cores, mem=pool.max_mem, gpus=pool.max_gpus)


# ---------------------------------------------------------------------------
# Allocation stores
# ---------------------------------------------------------------------------
def _decide(
    entries: dict[int, tuple[ResourceUsage, str]],
    req: ResourceUsage,
    kind: str,
    budget: Budget,
    max_oversize: int,
    reserve_pool: bool,
) -> bool:
    """Pure admission decision shared by every store implementation."""
    sum_normal = ResourceUsage(
        cores=sum(u.cores for u, k in entries.values() if k == NORMAL),
        mem=sum(u.mem for u, k in entries.values() if k == NORMAL),
        gpus=sum(u.gpus for u, k in entries.values() if k == NORMAL),
    )
    count_oversize = sum(1 for _, k in entries.values() if k == OVERSIZE)
    if kind == OVERSIZE:
        if count_oversize >= max_oversize:
            return False
        if reserve_pool and (sum_normal.cores or sum_normal.mem or sum_normal.gpus):
            return False
        return True
    # normal job
    if reserve_pool and count_oversize:
        return False
    fits = (
        (budget.cores is None or sum_normal.cores + req.cores <= budget.cores)
        and (budget.mem is None or sum_normal.mem + req.mem <= budget.mem)
        and (budget.gpus is None or sum_normal.gpus + req.gpus <= budget.gpus)
    )
    return fits


class AllocationStore(ABC):
    """Holds, per ``(pool, user)``, the ledger of admitted ``{job_id: allocation}``."""

    @abstractmethod
    def read(self, pool: str, user_id: int) -> dict[int, tuple[ResourceUsage, str]]:
        """Return the current ledger as ``{job_id: (allocation, kind)}``."""

    @abstractmethod
    def admit(
        self,
        pool: str,
        user_id: int,
        job_id: int,
        req: ResourceUsage,
        *,
        kind: str,
        budget: Budget,
        max_oversize: int,
        reserve_pool: bool,
        drop_job_ids: set[int],
    ) -> bool:
        """Atomically drop ``drop_job_ids`` (and ``job_id``'s own stale entry), then admit
        ``job_id`` iff it fits. Returns True (admitted, entry recorded) or False (deferred,
        ledger otherwise left as-is apart from the drops)."""


class InMemoryAllocationStore(AllocationStore):
    """Process-local store for tests and single-process deployments. Atomicity is provided by
    a lock; it deliberately mirrors :class:`ValkeyAllocationStore`'s semantics (minus TTL)."""

    def __init__(self, key_prefix: str = "tpv:pool", **_ignored: Any):
        self.key_prefix = key_prefix
        self._lock = threading.Lock()
        self._data: dict[str, dict[int, tuple[ResourceUsage, str]]] = {}

    def _key(self, pool: str, user_id: int) -> str:
        return f"{self.key_prefix}:{pool}:user:{{{user_id}}}"

    def read(self, pool: str, user_id: int) -> dict[int, tuple[ResourceUsage, str]]:
        with self._lock:
            return dict(self._data.get(self._key(pool, user_id), {}))

    def admit(
        self,
        pool: str,
        user_id: int,
        job_id: int,
        req: ResourceUsage,
        *,
        kind: str,
        budget: Budget,
        max_oversize: int,
        reserve_pool: bool,
        drop_job_ids: set[int],
    ) -> bool:
        key = self._key(pool, user_id)
        with self._lock:
            ledger = self._data.setdefault(key, {})
            for jid in drop_job_ids:
                ledger.pop(jid, None)
            ledger.pop(job_id, None)
            if _decide(ledger, req, kind, budget, max_oversize, reserve_pool):
                ledger[job_id] = (req, kind)
                return True
            return False


# Atomic admit for Valkey/Redis. Mirrors _decide(). KEYS[1] is the ledger key; ARGV is
# [job_id, rc, rm, rg, kind, bc, bm, bg, max_oversize, reserve, ttl, drop_id...] where a
# budget of -1 means "unlimited" for that dimension.
_ADMIT_LUA = """
local key = KEYS[1]
local job_id = ARGV[1]
local rc, rm, rg = tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local kind = ARGV[5]
local bc, bm, bg = tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8])
local max_oversize = tonumber(ARGV[9])
local reserve = tonumber(ARGV[10])
local ttl = tonumber(ARGV[11])
redis.call('HDEL', key, job_id)
for i = 12, #ARGV do redis.call('HDEL', key, ARGV[i]) end
local flat = redis.call('HGETALL', key)
local sum_c, sum_m, sum_g, count_oversize = 0, 0, 0, 0
for i = 1, #flat, 2 do
  local c, m, g, k = string.match(flat[i + 1], '([^|]*)|([^|]*)|([^|]*)|([^|]*)')
  if k == 'oversize' then
    count_oversize = count_oversize + 1
  else
    sum_c = sum_c + tonumber(c); sum_m = sum_m + tonumber(m); sum_g = sum_g + tonumber(g)
  end
end
local admit = false
if kind == 'oversize' then
  if count_oversize < max_oversize and (reserve == 0 or (sum_c == 0 and sum_m == 0 and sum_g == 0)) then
    admit = true
  end
else
  local fits = (bc < 0 or sum_c + rc <= bc) and (bm < 0 or sum_m + rm <= bm) and (bg < 0 or sum_g + rg <= bg)
  if fits and (reserve == 0 or count_oversize == 0) then admit = true end
end
if admit then
  redis.call('HSET', key, job_id, rc .. '|' .. rm .. '|' .. rg .. '|' .. kind)
end
if ttl > 0 and redis.call('EXISTS', key) == 1 then redis.call('EXPIRE', key, ttl) end
if admit then return 1 else return 0 end
"""


class ValkeyAllocationStore(AllocationStore):
    """Valkey/Redis-backed store. redis-py speaks the Valkey wire protocol.

    Keys are ``{key_prefix}:{pool}:user:{{user_id}}`` -- the ``{user_id}`` hash tag co-locates
    a user's pool keys on one cluster slot. Each ledger is a hash of
    ``job_id -> "cores|mem|gpus|kind"`` with a whole-key TTL as the orphan backstop.
    """

    def __init__(
        self,
        url: str = "valkey://localhost:6379/0",
        key_prefix: str = "tpv:pool",
        ttl: int = 3600,
        client: Any = None,
        **client_options: Any,
    ):
        try:
            import redis
        except ImportError as e:  # pragma: no cover - exercised only without redis installed
            raise StoreUnavailable("The 'redis' package is required for ValkeyAllocationStore") from e
        self._redis_mod = redis
        if client is not None:
            # Allow injecting a pre-built client (e.g. a fake) for tests / custom wiring.
            self.client = client
        else:
            # redis-py understands redis:// and rediss://; normalise the valkey:// alias.
            if url.startswith("valkey://"):
                url = "redis://" + url[len("valkey://") :]
            elif url.startswith("valkeys://"):
                url = "rediss://" + url[len("valkeys://") :]
            self.client = redis.from_url(url, decode_responses=True, **client_options)
        self.key_prefix = key_prefix
        self.ttl = ttl
        self._admit = self.client.register_script(_ADMIT_LUA)

    def _key(self, pool: str, user_id: int) -> str:
        return f"{self.key_prefix}:{pool}:user:{{{user_id}}}"

    def read(self, pool: str, user_id: int) -> dict[int, tuple[ResourceUsage, str]]:
        try:
            raw = self.client.hgetall(self._key(pool, user_id))
        except self._redis_mod.exceptions.RedisError as e:
            raise StoreUnavailable(str(e)) from e
        ledger = {}
        for jid, val in raw.items():
            c, m, g, kind = val.split("|")
            ledger[int(jid)] = (ResourceUsage(float(c), float(m), float(g)), kind)
        return ledger

    def admit(
        self,
        pool: str,
        user_id: int,
        job_id: int,
        req: ResourceUsage,
        *,
        kind: str,
        budget: Budget,
        max_oversize: int,
        reserve_pool: bool,
        drop_job_ids: set[int],
    ) -> bool:
        args = [
            job_id,
            req.cores,
            req.mem,
            req.gpus,
            kind,
            -1 if budget.cores is None else budget.cores,
            -1 if budget.mem is None else budget.mem,
            -1 if budget.gpus is None else budget.gpus,
            max_oversize,
            1 if reserve_pool else 0,
            self.ttl,
            *drop_job_ids,
        ]
        try:
            return bool(self._admit(keys=[self._key(pool, user_id)], args=args))
        except self._redis_mod.exceptions.RedisError as e:
            raise StoreUnavailable(str(e)) from e


class ResourcePoolManager:
    """Instantiates and holds the pluggable store and budget provider for a loaded config.

    One is created per mapper (see :class:`tpv.core.mapper.EntityToDestinationMapper`).
    """

    def __init__(self, config: ResourcePoolConfig):
        self.config = config
        self.store: AllocationStore = _load_class(config.store)(**(config.store_options or {}))
        self.provider: BudgetProvider = _load_class(config.budget_provider)(config)

    def pool(self, name: str) -> Pool | None:
        return self.config.pools.get(name)


def terminal_job_ids(sa_session: scoped_session[Session], job_ids: set[int]) -> set[int]:
    """Return the subset of ``job_ids`` whose Galaxy job has reached a terminal state.

    Takes the SQLAlchemy session (``app.model.context``) rather than the whole ``app`` so it
    depends only on what it uses. Read-only; absent/unknown job rows are *not* returned (kept
    in the ledger), so allocations are released only on positive evidence of completion.
    """
    if not job_ids:
        return set()
    rows = (
        sa_session.query(model.Job.id)
        .filter(model.Job.table.c.id.in_(list(job_ids)))
        .filter(model.Job.table.c.state.in_(TERMINAL_JOB_STATES))
    )
    return {row[0] for row in rows}

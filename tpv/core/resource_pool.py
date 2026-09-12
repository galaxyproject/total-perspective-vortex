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

The load-bearing operation is :meth:`AllocationStore.admit_many`, a single atomic
check-and-record: it drops finished jobs, sums the remaining committed usage and, if the
incoming job fits every matching pool budget (or oversize allowance), records it in all pools.
Because it is atomic, two concurrent maps for the same user cannot both exceed the budget.
"""

from __future__ import annotations

import importlib
import json
import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, NamedTuple, cast

from galaxy import model
from pydantic import BaseModel, ConfigDict, Field
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
# Configuration models
# ---------------------------------------------------------------------------
# Pool *policy* (budgets, oversize, tags) is expressed as first-class ``pools:`` entities in
# ``tpv.core.entities`` (see :class:`~tpv.core.entities.PoolEntity`). Only the *infrastructure*
# wiring for the allocation store lives here, declared under ``global.resource_pool_store``.
class OversizePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_concurrent: int = Field(
        default=0,
        description="Maximum concurrent oversize jobs per user in this pool; 0 rejects oversize requests.",
    )
    hard_max_cores: float | None = Field(default=None, description="Maximum cores requested by one oversize job.")
    hard_max_mem: float | None = Field(default=None, description="Maximum memory in GB requested by one oversize job.")
    hard_max_gpus: float | None = Field(default=None, description="Maximum GPUs requested by one oversize job.")
    reserve_pool: bool = Field(
        default=False,
        description="Prevent normal and oversize resource usage from coexisting in this pool; max_concurrent still applies.",
    )


class StoreConfig(BaseModel):
    """Infrastructure wiring for the allocation store, declared under
    ``global.resource_pool_store``. Only backend wiring belongs here -- a store class plus its
    options (url, key_prefix, ...); pool *policy* is expressed as ``pools:`` entities::

        global:
          resource_pool_store:
            class: tpv.core.resource_pool.ValkeyAllocationStore
            url: valkey://localhost:6379/0
    """

    # Extra keys (url, key_prefix, ...) are collected and passed to the store constructor.
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    store_class: str = Field(alias="class", default="tpv.core.resource_pool.ValkeyAllocationStore")

    def build_store(self) -> "AllocationStore":
        options = dict(self.__pydantic_extra__ or {})
        return cast("AllocationStore", _load_class(self.store_class)(**options))


def _load_class(dotted_path: str) -> Any:
    module_path, _, name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"Not a dotted class path: {dotted_path!r}")
    return getattr(importlib.import_module(module_path), name)


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


class PoolAdmission(NamedTuple):
    pool: str
    req: ResourceUsage
    kind: str
    budget: Budget
    max_oversize: int
    reserve_pool: bool
    drop_job_ids: set[int]


class AllocationStore(ABC):
    """Holds, per ``(pool, user)``, the ledger of admitted ``{job_id: allocation}``."""

    @abstractmethod
    def read(self, pool: str, user_id: int) -> dict[int, tuple[ResourceUsage, str]]:
        """Return the current ledger as ``{job_id: (allocation, kind)}``."""

    @abstractmethod
    def admit_many(self, user_id: int, job_id: int, admissions: list[PoolAdmission]) -> bool:
        """Atomically reconcile and admit a job to every requested pool, or none.

        Drop terminal IDs and the job's own previous entry from every requested pool first.
        On rejection those drops remain, but no new allocation is recorded in any pool.
        Each pool must appear at most once; all pools must belong to the same user.
        """

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
        """Admit to a single pool with the same atomic semantics as ``admit_many``."""
        return self.admit_many(
            user_id,
            job_id,
            [PoolAdmission(pool, req, kind, budget, max_oversize, reserve_pool, drop_job_ids)],
        )


class InMemoryAllocationStore(AllocationStore):
    """Process-local store for tests and single-process deployments.

    A lock covers all of a job's pools, mirroring the Valkey script's atomic admission.
    """

    def __init__(self, key_prefix: str = "tpv:pool", **_ignored: Any):
        self.key_prefix = key_prefix
        self._lock = threading.Lock()
        self._data: dict[str, dict[int, tuple[ResourceUsage, str]]] = {}

    def _key(self, pool: str, user_id: int) -> str:
        return f"{self.key_prefix}:{pool}:user:{{{user_id}}}"

    def read(self, pool: str, user_id: int) -> dict[int, tuple[ResourceUsage, str]]:
        with self._lock:
            return dict(self._data.get(self._key(pool, user_id), {}))

    def admit_many(self, user_id: int, job_id: int, admissions: list[PoolAdmission]) -> bool:
        with self._lock:
            ledgers = [self._data.setdefault(self._key(a.pool, user_id), {}) for a in admissions]
            for admission, ledger in zip(admissions, ledgers):
                for jid in admission.drop_job_ids | {job_id}:
                    ledger.pop(jid, None)
            if not all(
                _decide(ledger, a.req, a.kind, a.budget, a.max_oversize, a.reserve_pool)
                for a, ledger in zip(admissions, ledgers)
            ):
                return False
            for admission, ledger in zip(admissions, ledgers):
                ledger[job_id] = (admission.req, admission.kind)
            return True


# All keys use the same user hash tag, so this transaction also fits one Redis Cluster slot.
# ARGV[1] is the job ID; ARGV[2] is a JSON array of policies in KEYS order. Budgets use -1
# for unlimited dimensions. Job IDs inside the JSON remain strings to avoid Lua precision loss.
_ADMIT_LUA = """
local job_id = ARGV[1]
local admissions = cjson.decode(ARGV[2])
for i, key in ipairs(KEYS) do
  redis.call('HDEL', key, job_id)
  for _, jid in ipairs(admissions[i].drop_job_ids) do redis.call('HDEL', key, jid) end
  -- Live jobs may outlast any idle interval; remove TTLs left by older TPV versions.
  redis.call('PERSIST', key)
end
for i, key in ipairs(KEYS) do
  local a = admissions[i]
  local flat = redis.call('HGETALL', key)
  local sum_c, sum_m, sum_g, count_oversize = 0, 0, 0, 0
  for j = 1, #flat, 2 do
    local c, m, g, kind = string.match(flat[j + 1], '([^|]*)|([^|]*)|([^|]*)|([^|]*)')
    if kind == 'oversize' then
      count_oversize = count_oversize + 1
    else
      sum_c = sum_c + tonumber(c); sum_m = sum_m + tonumber(m); sum_g = sum_g + tonumber(g)
    end
  end
  if a.kind == 'oversize' then
    if count_oversize >= a.max_oversize then return 0 end
    if a.reserve_pool and (sum_c ~= 0 or sum_m ~= 0 or sum_g ~= 0) then return 0 end
  else
    local fits = (a.bc < 0 or sum_c + a.rc <= a.bc)
      and (a.bm < 0 or sum_m + a.rm <= a.bm)
      and (a.bg < 0 or sum_g + a.rg <= a.bg)
    if not fits or (a.reserve_pool and count_oversize > 0) then return 0 end
  end
end
-- Only write allocations after every pool has accepted the job.
for i, key in ipairs(KEYS) do
  local a = admissions[i]
  redis.call('HSET', key, job_id, a.rc .. '|' .. a.rm .. '|' .. a.rg .. '|' .. a.kind)
end
return 1
"""


class ValkeyAllocationStore(AllocationStore):
    """Valkey/Redis-backed store. redis-py speaks the Valkey wire protocol.

    Keys are ``{key_prefix}:{pool}:user:{{user_id}}`` -- the ``{user_id}`` hash tag co-locates
    a user's pool keys on one cluster slot. Each ledger is a hash of
    ``job_id -> "cores|mem|gpus|kind"``. Entries persist until job-state reconciliation
    confirms completion; an idle ledger can still belong to running jobs.
    """

    def __init__(
        self,
        url: str = "valkey://localhost:6379/0",
        key_prefix: str = "tpv:pool",
        ttl: int = 0,
        client: Any = None,
        **client_options: Any,
    ):
        if ttl != 0:
            raise ValueError("Resource pool ledgers cannot expire while jobs are running; remove ttl or set ttl: 0")
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

    def admit_many(self, user_id: int, job_id: int, admissions: list[PoolAdmission]) -> bool:
        if not admissions:
            return True
        policies = [
            {
                "rc": a.req.cores,
                "rm": a.req.mem,
                "rg": a.req.gpus,
                "kind": a.kind,
                "bc": -1 if a.budget.cores is None else a.budget.cores,
                "bm": -1 if a.budget.mem is None else a.budget.mem,
                "bg": -1 if a.budget.gpus is None else a.budget.gpus,
                "max_oversize": a.max_oversize,
                "reserve_pool": a.reserve_pool,
                "drop_job_ids": [str(jid) for jid in a.drop_job_ids],
            }
            for a in admissions
        ]
        try:
            return bool(
                self._admit(
                    keys=[self._key(a.pool, user_id) for a in admissions],
                    args=[job_id, json.dumps(policies)],
                )
            )
        except self._redis_mod.exceptions.RedisError as e:
            raise StoreUnavailable(str(e)) from e


class ResourcePoolManager:
    """Instantiates and holds the pluggable allocation store for a loaded config.

    This is pure infrastructure: pool *policy* (budgets, oversize, tags) lives on the
    first-class ``pools:`` entities and is resolved by the mapper. One manager is created per
    mapper (see :class:`tpv.core.mapper.EntityToDestinationMapper`).
    """

    def __init__(self, store: AllocationStore):
        self.store: AllocationStore = store


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

from galaxy.jobs import JobDestination

from tpv.core.explain import ExplainCollector
from tpv.rules import gateway

from .test import mock_galaxy


class TPVDryRunner:

    def __init__(
        self,
        job_conf: str,
        tpv_confs: str | None = None,
        user: mock_galaxy.User | None = None,
        tool: mock_galaxy.Tool | None = None,
        job: mock_galaxy.Job | None = None,
    ):
        self.galaxy_app = mock_galaxy.App(job_conf=job_conf, create_model=True)
        self.user = user
        self.tool = tool
        self.job = job
        if tpv_confs:
            self.tpv_config_files = tpv_confs
        else:
            tpv_config_list: list[str] = self.galaxy_app.job_config.get_destination(  # type: ignore[no-untyped-call]
                "tpv_dispatcher"
            ).params["tpv_config_files"]
            self.tpv_config_files = self.resolve_relative_config_paths(tpv_config_list, job_conf)

    @staticmethod
    def resolve_relative_config_paths(config_files: list[str], job_conf: str) -> list[str]:
        """Resolve relative tpv_config_files paths relative to the job_conf's directory."""
        job_conf_dir = os.path.dirname(os.path.abspath(job_conf))
        resolved: list[str] = []
        for path in config_files:
            if not os.path.isabs(path) and "://" not in path:
                resolved.append(os.path.join(job_conf_dir, path))
            else:
                resolved.append(path)
        return resolved

    def run(
        self, explain: bool = False
    ) -> JobDestination | Tuple[JobDestination | None, ExplainCollector | None]:
        gateway.ACTIVE_DESTINATION_MAPPERS = {}
        collector = ExplainCollector() if explain else None
        try:
            destination = gateway.map_tool_to_destination(
                self.galaxy_app,  # type: ignore[arg-type]
                self.job,  # type: ignore[arg-type]
                self.tool,  # type: ignore[arg-type]
                self.user,  # type: ignore[arg-type]
                tpv_config_files=self.tpv_config_files,
                explain_collector=collector,
            )
        except Exception:
            if explain:
                return None, collector
            raise
        if explain:
            return destination, collector
        return destination

    @staticmethod
    def from_params(
        job_conf: str,
        user_email: str | None = None,
        tool_id: str | None = None,
        roles: list[str] | None = None,
        history_tags: list[str] | None = None,
        tpv_confs: str | None = None,
        input_size: int | None = None,
    ) -> "TPVDryRunner":
        if user_email is not None:
            user = mock_galaxy.User(username="gargravarr", email=user_email)
        else:
            user = None

        if roles:
            if not user:
                user = mock_galaxy.User("gargravarr", "gargravarr@vortex.org")
            user.roles = [mock_galaxy.Role(role_name) for role_name in roles]

        if tool_id:
            tool = mock_galaxy.Tool(
                tool_id,
                version=tool_id.split("/")[-1] if "/" in tool_id else None,
            )
        else:
            tool = None

        job = mock_galaxy.Job()
        if input_size:
            dataset = mock_galaxy.DatasetAssociation(
                "test", mock_galaxy.Dataset("test.txt", file_size=input_size * 1024**3)
            )
            job.add_input_dataset(dataset)
        job.history = mock_galaxy.History()
        if history_tags:
            job.history.tags = [mock_galaxy.HistoryTag(tag_name) for tag_name in history_tags]
        return TPVDryRunner(job_conf=job_conf, tpv_confs=tpv_confs, user=user, tool=tool, job=job)

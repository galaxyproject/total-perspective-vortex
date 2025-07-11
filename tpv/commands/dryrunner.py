from typing import List, Optional

from galaxy.jobs import JobDestination

from tpv.rules import gateway

from .test import mock_galaxy


class TPVDryRunner:

    def __init__(
        self,
        job_conf: str,
        tpv_confs: Optional[str] = None,
        user: Optional[mock_galaxy.User] = None,
        tool: Optional[mock_galaxy.Tool] = None,
        job: Optional[mock_galaxy.Job] = None,
    ):
        self.galaxy_app = mock_galaxy.App(job_conf=job_conf, create_model=True)
        self.user = user
        self.tool = tool
        self.job = job
        if tpv_confs:
            self.tpv_config_files = tpv_confs
        else:
            self.tpv_config_files = self.galaxy_app.job_config.get_destination(  # type: ignore[no-untyped-call]
                "tpv_dispatcher"
            ).params["tpv_config_files"]

    def run(self) -> JobDestination:
        gateway.ACTIVE_DESTINATION_MAPPERS = {}
        return gateway.map_tool_to_destination(
            self.galaxy_app,  # type: ignore[arg-type]
            self.job,  # type: ignore[arg-type]
            self.tool,  # type: ignore[arg-type]
            self.user,  # type: ignore[arg-type]
            tpv_config_files=self.tpv_config_files,
        )

    @staticmethod
    def from_params(
        job_conf: str,
        user_email: Optional[str] = None,
        tool_id: Optional[str] = None,
        roles: Optional[List[str]] = None,
        history_tags: Optional[List[str]] = None,
        tpv_confs: Optional[str] = None,
        input_size: Optional[int] = None,
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

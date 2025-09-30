import hashlib
from typing import Any, Dict, List, Optional, cast

from galaxy.job_metrics import JobMetrics
from galaxy.jobs import JobConfiguration
from galaxy.model import mapping
from galaxy.structured_app import MinimalManagerApp
from galaxy.tool_util.deps.requirements import ResourceRequirement
from galaxy.util import bunch
from galaxy.web_stack import ApplicationStack


# App mock=======================================================
class Role:
    def __init__(self, name: str):
        self.name = name
        self.deleted = False


class HistoryTag:
    def __init__(self, user_tname: str):
        self.user_tname = user_tname


class History:
    def __init__(self, name: str = "Unnamed TPV dry run history", tags: List[str] = []):
        self.name = name
        self.tags = [HistoryTag(tag_name) for tag_name in tags]


class App:
    def __init__(self, job_conf: Optional[str] = None, create_model: bool = False):
        self.config = bunch.Bunch(
            job_config_file=job_conf,
            use_tasked_jobs=False,
            job_resource_params_file="/tmp/fake_absent_path",
            config_dict={},
            default_job_resubmission_condition="",
            track_jobs_in_database=True,
            server_name="main",
            is_set=lambda x: True,
            watch_job_rules="auto",
        )  # type: ignore[no-untyped-call]
        self.job_metrics = JobMetrics()  # type: ignore[no-untyped-call]
        if create_model:
            self.model = mapping.init("/tmp", "sqlite:///:memory:", create_tables=True)
        self.application_stack = ApplicationStack(app=self)  # type: ignore[no-untyped-call]
        self.job_config = JobConfiguration(cast(MinimalManagerApp, self))


class User:
    def __init__(self, username: str, email: str, roles: List[str] = [], id: Optional[int] = None):
        self.username = username
        self.email = email
        self.roles = [Role(name) for name in roles]
        self.id = id or int(hashlib.sha256(f"{self.username}".encode("utf-8")).hexdigest(), 16) % 1000000

    def all_roles(self) -> List[Role]:
        """
        Return a unique list of Roles associated with this user or any of their groups.
        """
        return self.roles


# Job mock and helpers=======================================
class Dataset:
    counter = 0

    def __init__(self, file_name: str, file_size: int, object_store_id: Optional[str] = None):
        self.id = self.counter
        self.counter += 1
        self.file_name = file_name
        self.file_size = file_size
        self.object_store_id = object_store_id

    def get_size(self, nice_size: bool = False, calculate_size: bool = False) -> int:
        return self.file_size


class DatasetAssociation:
    def __init__(self, name: str, dataset: Dataset):
        self.name = name
        self.dataset = dataset


class JobToInputDatasetAssociation:
    def __init__(self, name: str, dataset: DatasetAssociation):
        self.name = name
        self.dataset = dataset


class Job:
    def __init__(self) -> None:
        self.input_datasets: List[JobToInputDatasetAssociation] = []
        self.input_library_datasets: List[JobToInputDatasetAssociation] = []
        self.param_values: Dict[str, Any] = dict()
        self.parameters: Dict[str, Any] = {}
        self.history: Optional[History] = None

    def add_input_dataset(self, dataset_association: DatasetAssociation) -> None:
        self.input_datasets.append(JobToInputDatasetAssociation(dataset_association.name, dataset_association))

    def get_param_values(self, app: App) -> Dict[str, Any]:
        return self.param_values


class DynamicTool:
    def __init__(self, uuid: str):
        self.uuid = uuid


# Tool mock and helpers=========================================
class Tool:
    def __init__(
        self,
        id: str,
        version: Optional[str] = None,
        resource_requirements: Optional[List[ResourceRequirement]] = None,
        dynamic_tool: Optional[DynamicTool] = None,
    ):
        self.id = id
        self.old_id = id
        self.version = version
        self.installed_tool_dependencies: List[Any] = []
        self.resource_requirements = resource_requirements or []
        self.dynamic_tool = dynamic_tool

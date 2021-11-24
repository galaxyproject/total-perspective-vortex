import os

from galaxy.model import mapping
from galaxy.job_metrics import JobMetrics
from galaxy.jobs import JobConfiguration
from galaxy.util import bunch
from galaxy.web_stack import ApplicationStack


# Job mock and helpers=======================================
class Job:
    def __init__(self):
        self.input_datasets = []
        self.input_library_datasets = []
        self.param_values = dict()
        self.parameters = []

    def add_input_dataset(self, dataset_association):
        self.input_datasets.append(JobToInputDatasetAssociation(dataset_association.name, dataset_association))

    def get_param_values(self, app):
        return self.param_values


class JobToInputDatasetAssociation:
    def __init__(self, name, dataset):
        self.name = name
        self.dataset = dataset


class DatasetAssociation:
    def __init__(self, name, dataset):
        self.name = name
        self.dataset = dataset


class Dataset:
    def __init__(self, file_name, file_size):
        self.file_name = file_name
        self.file_size = file_size


# Tool mock and helpers=========================================
class Tool:
    def __init__(self, id):
        self.id = id
        self.old_id = id
        self.installed_tool_dependencies = []


# App mock=======================================================
class App:
    def __init__(self, job_conf='fixtures/job_conf.yml', create_model=False):
        self.config = bunch.Bunch(
            job_config_file=os.path.join(os.path.dirname(__file__), job_conf),
            use_tasked_jobs=False,
            job_resource_params_file="/tmp/fake_absent_path",
            config_dict={},
            default_job_resubmission_condition="",
            track_jobs_in_database=True,
            server_name="main",
        )
        self.application_stack = ApplicationStack()
        self.job_metrics = JobMetrics()
        self.job_config = JobConfiguration(self)
        if create_model:
            self.model = mapping.init(
                "/tmp",
                "sqlite:///:memory:",
                create_tables=True
            )


class User:
    def __init__(self, username, email, roles=[]):
        self.username = username
        self.email = email
        self.roles = [Role(name) for name in roles]

    def all_roles(self):
        """
        Return a unique list of Roles associated with this user or any of their groups.
        """
        return self.roles


class Role:
    def __init__(self, name):
        self.name = name
        self.deleted = False

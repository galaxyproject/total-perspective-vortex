from .test import mock_galaxy
from tpv.rules import gateway


class TPVDryRunner():

    def __init__(self, job_conf, tpv_confs=None, user=None, tool=None, job=None):
        self.galaxy_app = mock_galaxy.App(job_conf=job_conf, create_model=True)
        self.user = user
        self.tool = tool
        self.job = job
        if tpv_confs:
            self.tpv_config_files = tpv_confs
        else:
            self.tpv_config_files = self.galaxy_app.job_config.get_destination(
                'tpv_dispatcher').params['tpv_config_files']

    def run(self):
        gateway.ACTIVE_DESTINATION_MAPPER = None
        return gateway.map_tool_to_destination(self.galaxy_app, self.job, self.tool, self.user,
                                               tpv_config_files=self.tpv_config_files)

    @staticmethod
    def from_params(job_conf, user=None, tool=None, tpv_confs=None, input_size=None):
        if user is not None:
            email = user
            user = mock_galaxy.User('gargravarr', email)
        else:
            user = None

        if tool:
            tool = mock_galaxy.Tool(tool)
        else:
            tool = None

        job = mock_galaxy.Job()
        if input_size:
            dataset = mock_galaxy.DatasetAssociation(
                "test",
                mock_galaxy.Dataset("test.txt", file_size=input_size*1024**3))
            job.add_input_dataset(dataset)

        return TPVDryRunner(job_conf=job_conf, tpv_confs=tpv_confs, user=user, tool=tool, job=job)

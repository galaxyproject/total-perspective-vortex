import os
import unittest

from galaxy.tool_util.deps.requirements import ResourceRequirement

from tpv.commands.test import mock_galaxy
from tpv.core.resource_requirements import extract_resource_requirements_from_tool
from tpv.rules import gateway


class TestMapperResourceRequirements(unittest.TestCase):

    @staticmethod
    def _map_to_destination(
        tool,
        user,
        datasets,
        tpv_config_path=None,
        tpv_config_files=[],
        tpv_configs=None,
    ):
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), "fixtures/job_conf.yml"))
        job = mock_galaxy.Job()
        for d in datasets:
            job.add_input_dataset(d)
        if not tpv_config_files:
            tpv_config = tpv_config_path or os.path.join(
                os.path.dirname(__file__), "fixtures/mapping-resource-requirements.yml"
            )
            tpv_config_files = [tpv_config]
        gateway.ACTIVE_DESTINATION_MAPPERS = {}
        if tpv_configs:
            return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_configs=tpv_configs)
        else:
            return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=tpv_config_files)

    def test_extract_resource_requirements_from_tool_empty(self):
        tool = mock_galaxy.Tool("test_tool")
        result = extract_resource_requirements_from_tool(tool)
        self.assertEqual(result, {})

    def test_extract_resource_requirements_from_tool_cores(self):
        cores_req = ResourceRequirement("4", "cores_min")

        tool = mock_galaxy.Tool("test_tool", resource_requirements=[cores_req])
        result = extract_resource_requirements_from_tool(tool)
        self.assertEqual(result, {"cores": 4})

    def test_extract_resource_requirements_from_tool_memory(self):
        mem_req = ResourceRequirement("8192", "ram_min")

        tool = mock_galaxy.Tool("test_tool", resource_requirements=[mem_req])
        result = extract_resource_requirements_from_tool(tool)
        self.assertEqual(result, {"mem": 8192})

    def test_extract_resource_requirements_from_tool_gpus(self):
        gpu_req = ResourceRequirement("2", "cuda_device_count_min")

        tool = mock_galaxy.Tool("test_tool", resource_requirements=[gpu_req])
        result = extract_resource_requirements_from_tool(tool)
        self.assertEqual(result, {"gpus": 2})

    def test_extract_resource_requirements_from_tool_multiple(self):
        cores_req = ResourceRequirement("4", "cores_min")
        mem_req = ResourceRequirement("16384", "ram_min")
        gpu_req = ResourceRequirement("1", "cuda_device_count_min")

        tool = mock_galaxy.Tool("test_tool", resource_requirements=[cores_req, mem_req, gpu_req])
        result = extract_resource_requirements_from_tool(tool)
        expected = {"cores": 4, "mem": 16384, "gpus": 1}
        self.assertEqual(result, expected)

    def test_extract_resource_requirements_with_not_implemented_error(self):
        req = ResourceRequirement("$(4 * 4)", "cores_min")

        tool = mock_galaxy.Tool("test_tool", resource_requirements=[req])
        result = extract_resource_requirements_from_tool(tool)
        self.assertEqual(result, {})

    def test_tool_with_resource_requirements_mapping(self):
        cores_req = ResourceRequirement("4", "cores_min")
        mem_req = ResourceRequirement("8192", "ram_min")

        tool = mock_galaxy.Tool("test_tool_with_resources", resource_requirements=[cores_req, mem_req])
        user = mock_galaxy.User("test_user", "test@test.com")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

        destination = self._map_to_destination(tool, user, datasets)

        self.assertIsNotNone(destination)

    def test_tool_uuid_prefix_for_dynamic_tools(self):
        tool = mock_galaxy.Tool("test_tool", dynamic_tool=mock_galaxy.DynamicTool("12345-67890-abcdef"))
        tool.tool_type = "interactive"
        user = mock_galaxy.User("test_user", "test@test.com")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

        destination = self._map_to_destination(tool, user, datasets)

        self.assertIsNotNone(destination)

    def test_tool_without_uuid_uses_regular_id(self):
        tool = mock_galaxy.Tool("hisat")
        user = mock_galaxy.User("test_user", "test@test.com")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

        destination = self._map_to_destination(tool, user, datasets)

        self.assertEqual(destination.id, "local")

    def test_tool_provided_resource_merging_with_config(self):
        cores_req = ResourceRequirement("8", "cores_min")
        mem_req = ResourceRequirement("16384", "ram_min")

        tool = mock_galaxy.Tool("trinity", resource_requirements=[cores_req, mem_req])
        user = mock_galaxy.User("test_user", "test@test.com")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

        destination = self._map_to_destination(tool, user, datasets)

        self.assertIsNotNone(destination)
        self.assertEqual(destination.id, "k8s_environment")
        # resource requirements override via tool matching
        assert destination.params["native_spec"] == "--mem 16 --cores 4 --gpus 3"

    def test_default_entity_creation_with_resource_requirements(self):
        cores_req = ResourceRequirement("2", "cores_min")
        ram_min_req = ResourceRequirement("12", "ram_min")

        tool = mock_galaxy.Tool("nonexistent_tool", resource_requirements=[cores_req, ram_min_req])
        user = mock_galaxy.User("test_user", "test@test.com")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

        destination = self._map_to_destination(tool, user, datasets)

        self.assertIsNotNone(destination)
        assert destination.params["native_spec"] == "--mem 12 --cores 2"

    def test_tool_with_mixed_resource_requirements(self):
        cores_min_req = ResourceRequirement("2", "cores_min")
        cores_max_req = ResourceRequirement("8", "cores_max")
        ram_min_req = ResourceRequirement("4096", "ram_min")
        ram_max_req = ResourceRequirement("32768", "ram_max")

        tool = mock_galaxy.Tool(
            "test_tool", resource_requirements=[cores_min_req, cores_max_req, ram_min_req, ram_max_req]
        )
        result = extract_resource_requirements_from_tool(tool)
        expected = {"cores": 2, "max_cores": 8, "mem": 4096, "max_mem": 32768}
        self.assertEqual(result, expected)

    def test_default_entity_creation_overrides_resource_requirements(self):
        cores_req = ResourceRequirement("3", "cores_min")
        mem_req = ResourceRequirement("8", "ram_min")
        gpu_req = ResourceRequirement("2", "cuda_device_count_min")

        tool = mock_galaxy.Tool("nonexistent_tool", resource_requirements=[cores_req, mem_req, gpu_req])
        user = mock_galaxy.User("test_user", "test@test.com")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]

        destination = self._map_to_destination(tool, user, datasets)

        self.assertIsNotNone(destination)
        assert destination.params["native_spec"] == "--mem 8 --cores 3 --gpus 2"

    def test_user_defined_tool_correctly_routed(self):
        tool = mock_galaxy.Tool("user_defined", mock_galaxy.DynamicTool("08175037-030a-44c5-8468-c9d36cc29067"))
        tool.tool_type = "user_defined"
        user = mock_galaxy.User("test_user", "test@test.com")
        datasets = [mock_galaxy.DatasetAssociation("test", mock_galaxy.Dataset("test.txt", file_size=5 * 1024**3))]
        tpv_config_path = os.path.join(os.path.dirname(__file__), "fixtures/mapping-user-defined.yml")
        destination = self._map_to_destination(tool, user, datasets, tpv_config_files=[tpv_config_path])
        assert destination.id == "pulsar_environment"

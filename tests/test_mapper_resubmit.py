import os
from galaxy_test.driver.integration_util import IntegrationTestCase
from galaxy.webapps.base import webapp


class TestMapperResubmission(IntegrationTestCase):
    default_tool_conf = os.path.join(os.path.dirname(__file__), 'fixtures/resubmit/tool_conf_resubmit.xml')

    @classmethod
    def handle_galaxy_config_kwds(cls, config):
        config["config_dir"] = os.path.join(os.path.dirname(__file__), 'fixtures')
        config["job_config_file"] = "job_conf_resubmit.yml"
        config["tool_data_path"] = os.path.join(os.path.dirname(__file__), 'fixtures/resubmit')
        config["tool_data_table_config_path"] = os.path.join(os.path.dirname(__file__),
                                                             'fixtures/resubmit/tool_data_tables.xml.sample')
        config["data_manager_config_file"] = os.path.join(os.path.dirname(__file__),
                                                          'fixtures/resubmit/data_manager_conf.xml.sample')
        config["template_path"] = os.path.abspath(os.path.join(os.path.dirname(webapp.__file__), 'templates'))

    def _assert_job_passes(self, tool_id="exit_code_oom", resource_parameters=None):
        resource_parameters = resource_parameters or {}
        self._run_tool_test(tool_id, resource_parameters=resource_parameters)

    def _assert_job_fails(self, tool_id="exit_code_oom", resource_parameters=None):
        resource_parameters = resource_parameters or {}
        exception_thrown = False
        try:
            self._run_tool_test(tool_id, resource_parameters=resource_parameters)
        except Exception:
            exception_thrown = True

        assert exception_thrown

    # FIXME: Temporarily disable tests till https://github.com/galaxyproject/galaxy/issues/14021 is resolved.
    # def test_mapping_with_resubmission(self):
    #     self._assert_job_passes(tool_id="exit_code_oom_with_resubmit")
    #
    # def test_mapping_without_resubmission(self):
    #     self._assert_job_fails(tool_id="exit_code_oom_no_resubmit")

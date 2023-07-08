import os
import time
import tempfile
import pathlib
import responses
import shutil
import unittest
from galaxy.jobs.mapper import JobMappingException
from tpv.rules import gateway
from tpv.commands.test import mock_galaxy


def _map_to_destination(tool, user, datasets=[], tpv_config_path=None, job_conf=None, app=None):
    if job_conf:
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), job_conf))
    elif app:
        galaxy_app = app
    else:
        galaxy_app = mock_galaxy.App(job_conf=os.path.join(os.path.dirname(__file__), 'fixtures/job_conf.yml'))
    job = mock_galaxy.Job()
    for d in datasets:
        job.add_input_dataset(d)
    tpv_config = tpv_config_path or os.path.join(os.path.dirname(__file__), 'fixtures/mapping-rules.yml')
    gateway.ACTIVE_DESTINATION_MAPPER = None
    return gateway.map_tool_to_destination(galaxy_app, job, tool, user, tpv_config_files=[tpv_config])

def main():
        
    tool = mock_galaxy.Tool('fastp')
    user = mock_galaxy.User('jenkinsbot', 'jenkinsbot@unimelb.edu.au')
    datasets = [mock_galaxy.DatasetAssociation("input", mock_galaxy.Dataset("input.fastq",
                                                                            file_size=1000*1024**3))]
    rules_file = os.path.join(os.path.dirname(__file__), 'fixtures/scenario-locations.yml')
    # destination = _map_to_destination(tool, user, datasets, tpv_config_path=rules_file)
    destination = _map_to_destination(tool, user, datasets=datasets, tpv_config_path=rules_file,
                                            job_conf='fixtures/job_conf.yml')
    print("destination: ",destination)
    # t = {'test':1}
    # t.keys

main()
Shell Commands
==============

lint
----
TPV config files can be checked for linting errors using the ``tpv lint`` command.

.. code-block:: console

    tpv lint <url_or_path_to_config_file>

If linting is successful, a lint successful message will be displayed with an exit code of zero.
If the linting fails, a lint failed message with the relevant error will be displayed with an exit code of 1. For
example:

.. code-block:: console

    $ cat >good.yml <<EOF
    tools:
      default:
        cores: 1
        mem: cores * 3.9
        context:
          partition: normal
        params:
          native_specification: "--nodes=1 --ntasks={cores} --ntasks-per-node={cores} --mem={round(mem*1024)} --partition={partition}"
        scheduling:
          reject:
            - offline
        rules: []
    EOF
    $ tpv lint good.yml
    INFO : tpv.core.shell: lint successful.
    $ echo $?
    0

    cat >bad.yml <<EOF
    tools:
      - default:
          cores: 1
    EOF
    $ tpv lint bad.yml
    INFO : tpv.core.shell: lint failed.
    $ echo $?
    1

test
----

You can test that your TPV configuration returns the expected destination for a given tool and/or user using the ``tpv
test`` command.

.. code-block:: console

    tpv test --job-conf <path_to_galaxy_job_conf_file> [--tool <tool_id>] \
      [--user <user_name_or_email>] <tpv_config_file> [tpv_config_file ...]

For example:

.. code-block:: console

    $ tree
    .
    ├── 00_default_tool.yml
    ├── 05_tools.yml
    ├── 10_destinations.yml
    └── job_conf
        └── job_conf.yml

    1 directory, 4 files

.. code-block:: console

    $ tpv test --job-conf job_conf/job_conf.yml *.yml
    !!python/object:galaxy.jobs.JobDestination
    converted: false
    env:
    - {name: LC_ALL, value: C}
    id: slurm
    legacy: false
    params: {native_specification: --nodes=1 --ntasks=1 --ntasks-per-node=1 --mem=3994
        --partition=normal, outputs_to_working_directory: true, tmp_dir: true}
    resubmit: []
    runner: slurm
    shell: null
    tags: null
    url: null

.. code-block:: console

    $ tpv test --job-conf job_conf/job_conf.yml --tool trinity *.yml
    !!python/object:galaxy.jobs.JobDestination
    converted: false
    env:
    - {name: LC_ALL, value: C}
    - {name: TERM, value: vt100}
    - {execute: ulimit -c 0}
    - {execute: ulimit -u 16384}
    id: pulsar
    legacy: false
    params:
      default_file_action: remote_transfer
      dependency_resolution: remote
      jobs_directory: /scratch/pulsar/staging
      outputs_to_working_directory: false
      remote_metadata: false
      rewrite_parameters: true
      submit_native_specification: --nodes=1 --ntasks=20 --ntasks-per-node=20 --partition=xlarge
      transport: curl
    resubmit: []
    runner: pulsar
    shell: null
    tags: null
    url: null

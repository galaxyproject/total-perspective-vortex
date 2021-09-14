Configuring Galaxy
==================

Configuring Galaxy
------------------
1. First install the TotalPerspectiveVortex into your Galaxy virtual environment.

.. code-block:: shell

    cd <galaxy_home>
    source .venv/bin/activate
    pip install --upgrade total-perspective-vortex


2. Edit your `job_conf.yml` in the `<galaxy_home>/config` folder and add the
   highlighted sections to it.

   You will need to add your own value for the ``mapper_config_file`` to the
   file. Instructions on how to obtain your CloudLaunch API key are given below.

.. literalinclude:: ../samples/job_conf.yml
   :language: yaml
   :linenos:
   :emphasize-lines: 15,17-22

3. Add your own custom rules to the ``mapper_config_file`` following instructions in
   the next section.

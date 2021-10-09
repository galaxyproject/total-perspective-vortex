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

   You can refer to a local file for the ``vortex_config_files`` setting, or alternatively,
   provider a link to a remote url.

.. literalinclude:: ../samples/job_conf.yml
   :language: yaml
   :linenos:
   :emphasize-lines: 15,17-24

3. Add your own custom rules to your local ``vortex_config_file``, following instructions in
   the next section.

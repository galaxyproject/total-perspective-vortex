Configuring Galaxy
==================

Simple configuration
--------------------
1. First install the TotalPerspectiveVortex into your Galaxy virtual environment.

.. code-block:: shell

    cd <galaxy_home>
    source .venv/bin/activate
    pip install --upgrade total-perspective-vortex


2. Edit your `job_conf.yml` in the `<galaxy_home>/config` folder and add the
   highlighted sections to it.

   You can refer to a local file for the ``tpv_config_files`` setting, or alternatively,
   provider a link to a remote url.

.. literalinclude:: ../samples/job_conf.yml
   :language: yaml
   :linenos:
   :emphasize-lines: 15,17-24

3. Add your own custom rules to your local ``tpv_config_file``, following instructions in
   the next section.


Combining multiple remote and local configs
--------------------------------------------

TPV allows rules to be loaded from remote or local sources.

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 7-9,14-19

    tpv_dispatcher:
     runner: dynamic
     type: python
     function: map_tool_to_destination
     rules_module: tpv.rules
     tpv_config_files:
       - https://usegalaxy.org/shared_rules.yml
       - config/tpv_rules_australia.yml

The config files listed first are overridden by config files listed later. The normal rules of inheritance apply.
This allows a central database of common rules to be maintained, with individual, site-specific overrides.

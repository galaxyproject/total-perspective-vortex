############
Installation
############

Basic steps
-----------

1. ``pip install total-perspective-vortex`` into Galaxy's python virtual environment
2. Create the TPV job mapping yaml file, specifying resource allocation and job routing preferences
3. Configure Galaxy's ``job_conf.yml`` to use TPV
4. Submit jobs as usual

Configuring Galaxy
==================

1. First install TPV into your Galaxy virtual environment.

   TPV is a conditional dependency of Galaxy since Galaxy 22.05. If TPV is enabled in your Galaxy job configuration, it
   will automatically be installed into Galaxy's virtualenv. Otherwise, or if you wish to upgrade to a newer version of
   TPV, you can use the process below to install manually:

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
      - https://gxy.io/tpv/db-latest.yml
      - config/tpv_rules_australia.yml

The config files listed first are overridden by config files listed later. The normal rules of inheritance apply.
This allows a central database of common rules to be maintained, with individual, site-specific overrides.


Standalone Installation
-----------------------

If you wish to install TPV outside of Galaxy's virtualenv (e.g. to use the ``tpv lint`` command locally or in a CI/CD
pipeline), use the ``[cli]`` pip requirement specifier to make sure the necessary Galaxy dependency packages are also
installed. **This should not be used in the Galaxy virtualenv**:

.. code-block:: console

   $ pip install 'total-perspective-vortex[cli]'

Shell Commands
==============

lint
----
Vortex config files can be checked for linting errors using the vortex lint command.

.. code-block:: shell

    cd <galaxy_home>
    source .venv/bin/activate
    pip install --upgrade total-perspective-vortex
    vortex lint <url_or_path_to_config_file>

If linting is successful, a lint successful message will be displayed with an exit code of zero.
If the linting fails, a lint failed message with the relevant error will be displayed with an exit code of 1.

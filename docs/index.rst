.. image:: images/tpv-logo-wide.png

.. centered:: Dynamic rules for routing Galaxy entities to destinations

TotalPerspectiveVortex (TPV) is a plugin for the `Galaxy application`_ that can route
entities (Tools, Users, Roles) to appropriate destinations with appropriate resource
alloations (cores, gpus, memory), based on a configurable yaml file. For example, it could
allocate 8 cores and 32GB of RAM to a bwa-mem job, and route it to a Slurm cluster, while
allocating 2 cores and 4GB of RAM to an upload job, and route it to a local runner. These
rules can also be shared community-wide, imported at runtime by any Galaxy deployment, and
overridden locally when necessary.

How it works
------------
TPV can be plugged into Galaxy via ``job_conf.yml``. TPVs configuration file specifies how entities
(tools, users, roles) should be allocated resources (cores, gpus, memory) and in complex environments
with multiple job destinations, where to map the resulting jobs to (through a flexible
tagging system). Destinations can have arbitrary scheduling tags defined, and each entity can express a
preference or aversion to specific scheduling tags. This tagging affects how jobs are routed to
destinations. In addition, admins can also plugin arbitrary python based rules for making more complex
decisions, as well as custom ranking functions for choosing between matching destinations.

Shared database
---------------

A shared database of TPV rules are maintained in: https://github.com/galaxyproject/tpv-shared-database/
These rules are based on typical settings used in the usegalaxy.* federation, which you can override
based on local resource availability.

Getting Started
---------------

1. ``pip install total-perspective-vortex`` into Galaxy's python virtual environment
2. Create the TPV job mapping yaml file, specifying resource allocation and job routing preferences
3. Configure Galaxy's ``job_conf.yml`` to use TPV
4. Submit jobs as usual

Standalone Installation
-----------------------

If you wish to install TPV outside of Galaxy's virtualenv (e.g. to use the ``tpv lint`` command locally or in a CI/CD
pipeline), use the ``[cli]`` pip requirement specifier to make sure the necessary Galaxy dependency packages are also
installed. **This should not be used in the Galaxy virtualenv**:

.. code-block:: console

   $ pip install 'total-perspective-vortex[cli]'

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   topics/tpv_by_example.rst
   topics/advanced_topics.rst
   topics/concepts.rst
   topics/inner_workings.rst
   topics/shell_commands.rst
   topics/migration_guide.rst
   topics/faq.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _Galaxy application: https://galaxyproject.org/
.. _Dynamic Tool Destinations: https://training.galaxyproject.org/training-material/topics/admin/tutorials/job-destinations/tutorial.html
.. _Job Router: https://github.com/galaxyproject/usegalaxy-playbook/blob/c674b4795d63485392acd55bf6b4c7fb31754f5d/env/common/files/galaxy/dynamic_rules/job_router.py
.. _Sorting Hat: https://github.com/usegalaxy-eu/sorting-hat

Configuring Vortex Rules
========================

Simple configuration
--------------------

The simplest possible example of a useful vortex config might look like the following:

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 4

    tools:
      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/.*:
        cores: 12
        mem: cores*4
        gpus: 1

    destinations:
     slurm:
       cores: 16
       mem: 64
       gpus: 2
     general_pulsar_1:
       cores: 8
       mem: 32
       gpus: 1


Here, we define one tool and its resource requirements, the destinations available, and the total resources available
at each destination (optional). The tools are matched by tool id, and can be a regular expression. Note how
resource requirements can also be computed as python expressions. If resource requirements are defined at the
destination, vortex will check whether the job will fit. For example, hisat2 will not schedule on `general_pulsar_1`
as it has insufficient cores. If resource requirements are omitted in the tool or destination, it is considered a match.

Default inheritance
-------------------

Inheritance provides a mechanism for an entity to inherit properties from other entity, reducing repetition.

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 1-2,4-7

    global:
      default_inherits: default

    tools:
      default:
        cores: 2
        mem: 4
        params:
          nativeSpecification: "--nodes=1 --ntasks={cores} --ntasks-per-node={cores} --mem={mem*1024}"
      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/hisat2/2.1.0+galaxy7:
        cores: 12
        mem: cores*4
        gpus: 1


The `global` section is used to define global vortex properties. The `default_inherits` property defines a "base class"
for all tools to inherit from.

In this example, if the `bwa` tool is executed, it will match the `default` tool, as there are no other matches,
thus inheriting its resource requirements. The hisat2 tool will also inherit these defaults, but is explicitly
overriding cores, mem and gpus. It will inherit the `nativeSpecification` param.

Explicit inheritance
--------------------

Explicit inheritance provides a mechanism for exerting greater control over the inheritance chain.

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 1-2,4-7

    global:
      default_inherits: default

    tools:
      default:
        cores: 2
        mem: 4
        params:
          nativeSpecification: "--nodes=1 --ntasks={cores} --ntasks-per-node={cores} --mem={mem*1024}"
      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/.*:
        cores: 12
        mem: cores*4
        gpus: 1
      .*minimap2.*:
        inherits: https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/.*:
        cores: 8
        gpus: 0

In this example, the minimap2 tool explicitly inherits requirements from the hisat2 tool, which in turn inherits
the default tool. There is no limit to how deep the inheritance hierarchy can be.



Explicit inheritance
--------------------

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 1-2,14

    global:
      default_inherits: default

    tools:
      default:
        cores: 2
        mem: 4
        params:
          nativeSpecification: "--nodes=1 --ntasks={cores} --ntasks-per-node={cores} --mem={mem*1024}"
      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/.*:
        cores: 12
        mem: cores*4
        gpus: 1
      .*minimap2.*:
        inherits: https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/.*:
        cores: 8
        gpus: 0

In this example, the minimap2 tool explicitly inherits requirements from the hisat2 tool, which in turn inherits
the default tool. There is no limit to how deep the inheritance hierarchy can be.

Scheduling tags
---------------

Scheduling tags provide a means by which to control how entities match up, and can be used to route jobs to
preferred destinations, or to explicitly control which users can execute which tools, and where.

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 7-9,14-19

    tools:
      default:
        cores: 2
        mem: 4
        params:
          nativeSpecification: "--nodes=1 --ntasks={cores} --ntasks-per-node={cores} --mem={mem*1024}"
        scheduling:
          reject:
            - offline
      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/.*:
        cores: 4
        mem: cores*4
        gpus: 1
        scheduling:
          require:
          prefer:
            - highmem
          accept:
          reject:
      https://toolshed.g2.bx.psu.edu/repos/iuc/minimap2/.*:
        cores: 4
        mem: cores*4
        gpus: 1
        scheduling:
          require:
            - highmem

    destinations:
     slurm:
       cores: 16
       mem: 64
       gpus: 2
       scheduling:
          prefer:
            - general

     general_pulsar_1:
       cores: 8
       mem: 32
       gpus: 1
       scheduling:
          prefer:
            - highmem
          reject:
            - offline

In this example, all tools reject destinations marked as offline. The hisat2 tool expresses a preference for highmem,
and inherits the rejection of offline tags. Inheritance can be used to override scheduling tags. For example, the
minimap2 tool inherits hisat2, but now requires a highmem tag, instead of merely preferring it.

The destinations themselves can be tagged in similar ways. In this case, the `general_pulsar_1` destination also
prefers the highmem tag, and thus, the hisat2 tool would schedule there. However, `general_pulsar_1` also rejects the
offline tag, and therefore, the hisat2 tool cannot schedule there. Therefore, it schedules on the only available
destination, which is slurm.

The minimap2 tool meanwhile requires highmem, but rejects offline tags, which leaves it nowhere to schedule.
This results in a JobMappingException being thrown.

A full table of how scheduling tags match up can be found in the _`Scheduling` section.

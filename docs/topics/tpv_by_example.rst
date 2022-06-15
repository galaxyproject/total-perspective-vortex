TPV by example
==============

Simple configuration
--------------------

The simplest possible example of a useful TPV config might look like the following:

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 4

    tools:
      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/.*:
        cores: 12
        mem: cores * 4
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
destination, TPV will check whether the job will fit. For example, hisat2 will not schedule on `general_pulsar_1`
as it has insufficient cores. If resource requirements are omitted in the tool or destination, it is considered a match.

Default inheritance
-------------------

Inheritance provides a mechanism for an entity to inherit properties from another entity, reducing repetition.

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
        mem: cores * 4
        gpus: 1


The `global` section is used to define global TPV properties. The `default_inherits` property defines a "base class"
for all tools to inherit from.

In this example, if the `bwa` tool is executed, it will match the `default` tool, as there are no other matches,
thus inheriting its resource requirements. The hisat2 tool will also inherit these defaults, but is explicitly
overriding cores, mem and gpus. It will inherit the `nativeSpecification` param.

Explicit inheritance
--------------------

Explicit inheritance provides a mechanism for exerting greater control over the inheritance chain.

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 1-2,15

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
        mem: cores * 4
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
        mem: cores * 4
        gpus: 1
        scheduling:
          require:
          prefer:
            - highmem
          accept:
          reject:
      https://toolshed.g2.bx.psu.edu/repos/iuc/minimap2/.*:
        cores: 4
        mem: cores * 4
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


Rules
-----

Rules provide a means by which to conditionally change entity requirements.

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 5-8,13-25

    tools:
      default:
        cores: 2
        mem: cores * 3
        rules:
          - id: my_overridable_rule
            if: input_size < 5
            fail: We don't run piddling datasets of {input_size}GB
      bwa:
        scheduling:
          require:
            - pulsar
        rules:
          - id: my_overridable_rule
            if: input_size < 1
            fail: We don't run piddling datasets
          - if: input_size <= 10
            cores: 4
            mem: cores * 4
            execute: |
               from galaxy.jobs.mapper import JobNotReadyException
               raise JobNotReadyException()
          - if: input_size > 10 and input_size < 20
            scheduling:
              require:
                - highmem
          - if: input_size >= 20
            fail: Input size: {input_size} is too large shouldn't run

The if clause can contain arbitrary python code, including multi-line python code. The only requirement is that the
last statement in the code block must evaluate to a boolean value. In this example, the `input_size` variable is an
automatically available contextual variable which is computed by totalling the sizes of all inputs to the job.
Additional available variables include app, job, tool, and user.

If the rule matches, the properties of the rule override the properties of the tool. For example, if the input_size
is 15, the bwa tool will require both pulsar and highmem tags.

Rules can be overridden by giving them an id. For example, the default for all tools is to reject input sizes < 5
by using the `my_overridable_rule` rule. We override that for the bwa tool by specifically referring to the inherited
rule by id. If no id is specified, an id is auto-generated and no longer overridable.

Note the use of the {input_size} variable in the fail message. The general rule is that all non-string expressions
are evaluated as python code blocks, while string variables are evaluated as python f-strings.

The execute block can be used to create arbitrary side-effects if a rule matches. The return value of an execute
block is ignored.

User and Role Handling
------------------------

Scheduling rules can also be expressed for users and roles.

.. code-block:: yaml
   :linenos:
   :emphasize-lines: 7-9,14-19

    tools:
      default:
        scheduling:
          require: []
          prefer:
            - general
          accept:
          reject:
            - pulsar
        rules: []
      dangerous_interactive_tool:
        cores: 8
        mem: 8
        scheduling:
          require:
            - authorize_dangerous_tool
    users:
      default:
        scheduling:
          reject:
            - authorize_dangerous_tool
      fairycake@vortex.org:
        cores: 4
        mem: 16
        scheduling:
          accept:
            - authorize_dangerous_tool
          prefer:
            - highmem

    roles:
      training.*:
        cores: 5
        mem: 7
        scheduling:
          reject:
            - pulsar

In this example, if user `fairycake@vortex.org` attempts to dispatch a `dangerous_interactive_tool` job, the
requirements for both entities would be combined. Most requirements would simply be merged, such as env vars
and job params. However, when combining gpus, cores and mem, the lower of the two values are used. In this case,
the combined entity would have a core value of 4 and a mem value of 8. This allows training users for example,
to be forced to use a lower number of cores than usual.

In addition, for these entities to be combined, the scheduling tags must also be compatible.
In this instance the `dangerous_interactive_tool` requires the `authorize_dangerous_tool` tag, which all users by
default reject. Therefore, most users cannot run this tool by default. However, `fairycake@vortex.org` overrides that
and accepts the `authorize_dangerous_tool` allowing only that user to run the dangerous tool.

Roles can be matched in this exact way. Rules can also be defined at the user and role level.

Metascheduling
--------------

Custom rank functions can be used to implement metascheduling capabilities. A rank function is used to select
the best matching destination from a list of matching destination. If no rank function is provided, the default
rank function simply chooses the most preferred destination out of the available destinations.

When more sophisticated control over scheduling is required, a rank function can be implemented through custom
python code.

.. code-block:: yaml
   :linenos:

    tools:
     default:
       cores: 2
       mem: 8
       rank: |
         import requests

         params = {
           'pretty': 'true',
           'db': 'pulsar-test',
           'q': 'SELECT last("percent_allocated") from "sinfo" group by "host"'
         }

         try:
           response = requests.get('http://stats.genome.edu.au:8086/query', params=params)
           data = response.json()
           cpu_by_destination = {s['tags']['host']:s['values'][0][1] for s in data.get('results')[0].get('series', [])}
           # sort by destination preference, and then by cpu usage
           candidate_destinations.sort(key=lambda d: (-1 * d.score(entity), cpu_by_destination.get(d.id)))
           final_destinations = candidate_destinations
         except Exception:
           log.exception("An error occurred while querying influxdb. Using a weighted random candidate destination")
           final_destinations = helpers.weighted_random_sampling(candidate_destinations)
         final_destinations


In this example, the rank function queries a remote influx database to find the least loaded destination, The matching
destinations are available to the rank function through the `candidate_destinations` contextual variable. Therefore,
in this example, the candidate destinations are first sorted by the best matching destination (score is the
default ranking function), and then sorted by CPU usage per destination, obtained from the influxdb query.

Note that the final statement in the rank function must be the list of sorted destinations.

Custom contexts
---------------
In addition to the automatically provided context variables (see :doc:`concepts`), TPV allows you to define arbitrary
custom variables, which are then available whenever an expression is evaluated. Contexts can be defined both globally
or at the level of each entity, with entity level context variables overriding global ones.

.. code-block:: yaml
   :linenos:

    global:
      default_inherits: default
      context:
        ABSOLUTE_FILE_SIZE_LIMIT: 100
        large_file_size: 10
        _a_protected_var: "some value"

    tools:
      default:
        context:
          additional_spec: --my-custom-param
        cores: 2
        mem: 4
        params:
          nativeSpecification: "--nodes=1 --ntasks={cores} --ntasks-per-node={cores} --mem={mem*1024} {additional_spec}"
         rules:
          - if: input_size >= ABSOLUTE_FILE_SIZE_LIMIT
            fail: Job input: {input_size} exceeds absolute limit of: {ABSOLUTE_FILE_SIZE_LIMIT}
          - if: input_size > large_file_size
            cores: 10

      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/hisat2/2.1.0+galaxy7:
        context:
          large_file_size: 20
          additional_spec: --overridden-param
        mem: cores * 4
        gpus: 1


In this example, three global context variables are defined, which are made available to all entities.
Variable names follow Python conventions, where all uppercase variables indicate constants that cannot be overridden.
Lower case indicates a public variable that can be overridden and changed, even across multiple TPV config files.
An underscore indicates a protected variable that can be overridden within the same file, but not across files.

Additional, the tool defaults section defines an additional context variable named 'additional_spec`, which is only
available to inheriting tools.

If we were to dispatch a job, say bwa, with an input_size of 15, the large file rule in the defaults section would
kick in, and the number of cores would be set to 10. If we were to dispatch a hisat2 job with the same input size
however, the large_file_size rule would not kick in, as it has been overridden to 20. The main takeaway from this
example is that variables are bound late, and therefore, rules and params can be crafted to allow inheriting
tools to conveniently override values, even across files. While this capability can be powerful, it needs to be
treated with the same care as any global variable in a programming language.

Multiple matches
---------------
If multiple regular expressions match, the matches are applied in order of appearance. Therefore, the convention is
to specify more general rule matches first, and more specific matches later. This matching also applies across
multiple TPV config files, again based on order of appearance.

.. code-block:: yaml
   :linenos:

    tools:
      default:
        cores: 2
        mem: 4
        params:
          nativeSpecification: "--nodes=1 --ntasks={cores} --ntasks-per-node={cores} --mem={mem*1024}"

      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/hisat2/*:
        mem: cores * 4
        gpus: 1

      https://toolshed.g2.bx.psu.edu/repos/iuc/hisat2/hisat2/2.1.0+galaxy7:
        env:
           MY_ADDITIONAL_FLAG: "test"


In this example, dispatching a hisat2 job would result in a mem value of 8, with 1 gpu. However, dispatching
the specific version of `2.1.0+galaxy7` would result in the additional env variable, with mem remaining at 8.

Job Resubmission
----------------
TPV has explict support for job resubmissions, so that advanced control over job resubmission is possible.

.. code-block:: yaml
   :linenos:

    tools:
      default:
        cores: 2
        mem: 4 * int(job.destination_params.get('SCALING_FACTOR', 1)) if job.destination_params else 1
        params:
          SCALING_FACTOR: "{2 * int(job.destination_params.get('SCALING_FACTOR', 2)) if job.destination_params else 2}"
        resubmit:
          with_more_mem_on_failure:
            condition: memory_limit_reached and attempt <= 3
            destination: tpv_dispatcher

In this example, we have defined a resubmission handler that resubmits the job if the memory limited is reached.
Note that the resubmit section looks exactly the same as Galaxy's, except that it follows a dictionary structure
instead of being a list. Refer to the Galaxy job configuration docs for more information on resubmit handlers. One
twist in this example is that we automatically increase the amount of memory provided to the job on each resubmission.
This is done by setting the SCALING_FACTOR param, which is a custom parameter which we have chosen for this example,
that we increase on each resubmission. Since each resubmission's destination is TPV, the param is re-evaluated on each
resubmission, and scaled accordingly. The memory is allocated based on the scaling factor, which therefore, also
scales accordingly.

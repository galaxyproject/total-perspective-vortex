###############
Advanced Topics
###############

Expressions
===========

Most TPV properties can be expressed as Python expressions. The rule of thumb is that all string expressions
are evaluated as python f-strings, and all integers or boolean expressions are evaluated as python code blocks.
For example, cpu, cores and mem are evaluated as python code blocks, as they evaluate to integer/float values.
However, env and params are evaluated as f-strings, as they result in string values. This is to improve the readability
and syntactic simplicity of TPV config files.

At the point of evaluating these functions, there is an evaluation context, which is a default set of variables
that are available to that expression. The following default variables are available to all expressions:

Default evaluation context
--------------------------
+----------+-----------------------------------------------------------------------------+
| Variable | Description                                                                 |
+==========+=============================================================================+
| app      | the Galaxy App object                                                       |
+----------+-----------------------------------------------------------------------------+
| tool     | the Galaxy tool object                                                      |
+----------+-----------------------------------------------------------------------------+
| user     | the current Galaxy user object                                              |
+----------+-----------------------------------------------------------------------------+
| job      | the Galaxy job object                                                       |
+----------+-----------------------------------------------------------------------------+
| mapper   | the TPV mapper object, which can be used to access parsed TPV configs       |
+----------+-----------------------------------------------------------------------------+
| entity   | the TPV entity being currently evaluated. Can be a combined entity.         |
+----------+-----------------------------------------------------------------------------+
| self     | an alias for the current TPV entity.                                        |
+----------+-----------------------------------------------------------------------------+

Custom evaluation contexts
---------------------------
These are user defined context values that can be defined globally, or locally at the level of each
entity. Any defined context value is available as a regular variable at the time the entity is evaluated.


Special evaluation contexts
---------------------------
In addition to the defaults above, additional context variables are available at different steps.

*gpu, core and mem expressions* - these are evaluated in order, and thus can be referred to in that same order.
For example, gpu expressions cannot refer to core and mem, as they have not been evaluated yet. cpu
expressions can be based on gpu values. mem expressions can refer to both cores and gpus.

*env and param expressions* - env expressions can be based on gpu, cores or mem. param expressions can additional
refer to evaluated env expressions.

*rank functions* - these can refer to all prior expressions, and are additional passed in a `candidate_destinations`
array, which is a list of matching TPV destinations.

Properties that do not support expressions
------------------------------------------

Some properties do not support expressions. These are primarily:

* max_accepted_cores, max_accepted_mem and max_accepted_gpus, which can only be defined on destinations. This is
  because when a combined entity is matched with a destination, concrete values are required.
* tags defined on entities

Evaluation by expression type
-----------------------------

The simple rule of thumb here is that all string expressions are evaluated as python f-strings,
and all integers or boolean expressions are evaluated as python code blocks. If evaluated as an
f-string, the expressions must be a single line and must evaluate to a string. If evaluated as
a code-block, expressions may span multiple lines of arbitrary Python code, but the last line must
be an expression that evaluates to the expected return type (The return statement should not and cannot
be used)

+--------------------+---------------+----------------------+
| Field              | Evaluated As  | Expected type        |
+====================+===============+======================+
| gpus               | code block    | float                |
+--------------------+---------------+----------------------+
| cores              | code block    | float                |
+--------------------+---------------+----------------------+
| mem                | code block    | float                |
+--------------------+---------------+----------------------+
| env                | f-strings     | string               |
+--------------------+---------------+----------------------+
| params             | f-strings     | string               |
+--------------------+---------------+----------------------+
| min_gpus           | code block    | float                |
+--------------------+---------------+----------------------+
| min_cores          | code block    | float                |
+--------------------+---------------+----------------------+
| min_mem            | code block    | float                |
+--------------------+---------------+----------------------+
| max_gpus           | code block    | float                |
+--------------------+---------------+----------------------+
| max_cores          | code block    | float                |
+--------------------+---------------+----------------------+
| max_mem            | code block    | float                |
+--------------------+---------------+----------------------+
| rank               | code block    | list of destinations |
+--------------------+---------------+----------------------+
| context            | not evaluated | string               |
+--------------------+---------------+----------------------+
| scheduling tags    | not evaluated | string               |
+--------------------+---------------+----------------------+
| inherits           | not evaluated | string               |
+--------------------+---------------+----------------------+
| max_accepted_gpus  | not evaluated | float                |
+--------------------+---------------+----------------------+
| max_accepted_cores | not evaluated | float                |
+--------------------+---------------+----------------------+
| max_accepted_mem   | not evaluated | float                |
+--------------------+---------------+----------------------+
| if                 | code block    | bool                 |
+--------------------+---------------+----------------------+
| rules              | not evaluated | list of rules        |
+--------------------+---------------+----------------------+
| execute            | code block    | void                 |
+--------------------+---------------+----------------------+
| fail               | f-string      | string               |
+--------------------+---------------+----------------------+
| resubmit           | f-strings     | string               |
+--------------------+---------------+----------------------+


Scheduling
==========

TPV offers several mechanisms for controlling scheduling, all of which are optional.
In its simplest form, no scheduling constraints would be defined at all, in which case
the entity would schedule on the first available destination. Admins can use scheduling tags to exert additional control
over which destinations jobs can schedule on. Scheduling tags fall into one of four categories,
(required, preferred, accepted, rejected), ranging from indicating a requirement for a particular entity,
to indicating complete aversion.

+-----------+--------------------------------------------------------------------------------------------------------+
| Tag Type  | Description                                                                                            |
+===========+========================================================================================================+
| require   | required tags must match up for scheduling to occur. For example, if a tool is marked as requiring the |
|           | `high-mem` tag, only destinations that are tagged as requiring, preferring or accepting the            |
|           | `high-mem` tag would be considering for scheduling.                                                    |
+-----------+--------------------------------------------------------------------------------------------------------+
| prefer    | prefer tags are ranked higher that accept tags when scheduling decisions are made.                     |
+-----------+--------------------------------------------------------------------------------------------------------+
| accept    | accept tags can be used to indicate that a entity can match up or support another entity, even         |
|           | if not preferentially.                                                                                 |
+-----------+--------------------------------------------------------------------------------------------------------+
| reject    | reject tags cannot be present for scheduling to occur. For example, if a tool is marked as rejecting   |
|           | the `pulsar` tag, only destinations that do not have that tag are considered for scheduling. If two    |
|           | entities have the same reject tag, they still repel each other.                                        |
+-----------+--------------------------------------------------------------------------------------------------------+


Scheduling tag compatibility table
----------------------------------

+------------+---------+--------+--------+--------+------------+
| Tag Type   | Require | Prefer | Accept | Reject | Not Tagged |
+============+=========+========+========+========+============+
| Require    |    ✓    |    ✓   |    ✓   |   ✕    |     ✕      |
+------------+---------+--------+--------+--------+------------+
| Prefer     |    ✓    |    ✓   |    ✓   |   ✕    |     ✓      |
+------------+---------+--------+--------+--------+------------+
| Accept     |    ✓    |    ✓   |    ✓   |   ✕    |     ✓      |
+------------+---------+--------+--------+--------+------------+
| Reject     |    ✕    |    ✕   |    ✕   |   ✕    |     ✓      |
+------------+---------+--------+--------+--------+------------+
| Not Tagged |    ✕    |    ✓   |    ✓   |   ✓    |     ✓      |
+------------+---------+--------+--------+--------+------------+


Scheduling by tag match
------------------------
Scheduling tags can be used to model anything from compatibility with a destination, to
permissions to execute a tool. (e.g. a tool can be tagged as requiring the "restricted"
tag, and users can be tagged as rejecting the "restricted" tag by default. Then, only users
who are specifically marked as requiring, tolerating, or preferring the "restricted" tag
can execute that tool. Of course, the destination must also be marked as not rejecting the
"restricted" tag.

Auto-injected tool type tags
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
TPV automatically adds a tool type tag to each mapped tool as an ``accept`` tag, using the form
``tool_type_<tool.tool_type>``. This allows administrators to route tools, such as expression
tools, interactive tools and user-defined tools by tag, to specific destinations.

In addition, as a default security measure, all destinations are treated as rejecting
``tool_type_user_defined`` by default. This means user-defined tools must be explicitly
accepted by a destination to be routable there.

For example:

.. code-block:: yaml

   destinations:
     local:
       runner: local
       scheduling:
         reject:
           - tool_type_interactive
     pulsar_user_tools:
       runner: pulsar
       scheduling:
         accept:
           - tool_type_user_defined

Auto-injected tool resource requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If a Galaxy tool wrapper defines ``resource_requirements``, TPV injects those values into the mapped tool
entity automatically. This lets tools that declare requirements in Galaxy XML wrappers participate in TPV
destination matching even when no explicit TPV tool resource fields are defined.

Mapped fields are:

* ``cores_min`` -> ``cores``
* ``cores_max`` -> ``max_cores``
* ``ram_min`` -> ``mem``
* ``ram_max`` -> ``max_mem``
* ``cuda_device_count_min`` -> ``gpus``
* ``cuda_device_count_max`` -> ``max_gpus``

If the same resource is also defined in TPV ``tools:``, the TPV configuration value overrides the
auto-injected value.

For example, with a tool that declares ``cores_min=8`` and ``ram_min=16384`` in Galaxy's tool XML:

.. code-block:: yaml
   :linenos:

   tools:
     default:
      mem: 8  # The Galaxy tool wrapper's ram_min would override this default value
     my_tool:
       mem: 32  # but this specific override takes priority over ram_min
   destinations:
     slurm:
       runner: slurm
       max_accepted_cores: 16
       max_accepted_mem: 32768

Note that tool resource requirements override tool defaults.

Scheduling by rules
-------------------
Rules can be used to conditionally modify any entity requirement. Rules can be given an ID,
which can subsequently be used by an inheriting entity to override the rule. If no ID is
specified, a unique ID is generated, and the rule can no longer be overridden. Rules
are typically evaluated through an `if` clause, which specifies the logical condition under
which the rule matches. If the rule matches, cores, memory, scheduling tags etc. can be
specified to override inherited values. The special clause `fail` can be used to immediately
fail the job with an error message. The `execute` clause can be used to execute an arbitrary
code block on rule match.

Scheduling by custom ranking functions
--------------------------------------
The default rank function sorts destinations by scoring how well the tags match the job's requirements.
As this may often be too simplistic, the rank function can be overridden by specifying a custom
rank clause. The rank clause can contain an arbitrary code block, which can do the desired sorting,
for example by determining destination load by querying the job manager, influx statistics etc.
The final statement in the rank clause must be the list of sorted destinations.

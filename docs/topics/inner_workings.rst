##############
Inner workings
##############

Types of objects
================

Conceptually, TPV consists of the following types of objects.

1. **Entities** - An entity is anything that will be considered for scheduling
by TPV. Entities include Tools, Users, Groups, Rules and Destinations.
All entities have some common properties (id, cores, mem, env, params,
and scheduling tags).

2. **Scheduling Tags** - Entities can have scheduling tags defined on them that determine which
entities match up, and which destination they can schedule on. Tags fall into one of four categories,
(required, preferred, accepted, rejected), ranging from indicating a requirement for a particular destination,
to indicating complete aversion.

3. **Loader** - The loader is responsible for loading entity definitions from a config file.
The loader will parse and validate entity definitions, including compiling python expressions,
and processing inheritance, to produce a list of entities suitable for mapping. The loader is also
capable of loading config files from multiple sources, including https urls.

4. **Mapper** - The mapper is responsible for routing a Galaxy job to its destination, based on the current user,
tool and job that must be scheduled. The mapper will respect the scheduling constraints expressed by the
loaded entities.


Operations
==========

When a mapper routes jobs to a destination, it does so by applying 5 basic operations on entities.

1. Inherit
----------
The inherit operation enables an entity to inherit the properties of another entity of the same
type, and to override any required properties. While a Tool can inherit another tool, which can in-turn inherit
yet another tool, it cannot inherit a User, as it is a different entity type. It is also possibly to globally define
a `default_inherits` field, which is the entity that all entities will inherit from should they
not have an `inherits` tag explicitly defined. Inheritance is generally processed at load time by the `Loader`,
so that there is no cost at runtime. However, the `Mapper` will process default inheritance, should the user, role
or tool that is being dispatched not have an entry in the entities list.

When inheriting scheduling tags, if the same tag is defined by both the parent and the child, only the child's
tag will take effect. For example, if a parent defines `high-mem` as a required tag, but a child defines `high-mem`
as a preferred tag, then the tag will be treated as a preferred tag.


2. Combine
----------
The combine operation matches up the current user, role and tool entities, and creates a combined
entity that shares all their respective preferences. If the same property is defined on both entities, the entity
with the higher merge priority will override the other. The priority order is fixed in the following way:
Destination > User > Role > Tool.
For example, if a tool specifies `cores`, and a user also specifies `cores`, the user's `cores` value will take
precedence. Properties defined on destinations have the highest priority of all.
The combine operation follows the following additional rules:

Combining scheduling tags
^^^^^^^^^^^^^^^^^^^^^^^^^
When combining scheduling tags, if a role expresses a preferences for tag `training` for example, and a tool expresses
a requirement for tag `high-mem`, the combined entity would share both preferences. This can be used to route certain
roles or users to specific destinations for example.

However, if the tags are mutually exclusive, then an IncompatibleTagsException is raised. For example, if a role
expresses a preference for training, but the tool rejects tag `training`, then the job can no longer be scheduled.
If the tags are compatible, then the tag with the stronger claim takes effect. For example, if a tool requires
`high-mem` and a user prefers `high-mem`, then the combined entity will require `high-mem`. An example of using
this property would be to restrict the availability of dangerous tools only to trusted users.

Combining envs and params
^^^^^^^^^^^^^^^^^^^^^^^^^
In this case, these requirements are simply merged, with duplicate envs and params merged in the following order:
Destination > User > Role > Tool.

3. Evaluate
-----------
This operation evaluates any python expressions in the combined entity. At this point, rules are also evaluated.
After evaluation, expressions such as cores, mem, max_cores, min_gpus etc., will all have concrete values. During
evaluation, the cores, mem and gpu values are clamped between min_cores, min_mem, min_gpus and max_cores, max_mem,
max_gpus. Afterwards, these values can be compared with a destination's values, as described in the `match` step next.

4. Match
--------
The match operation is used to find matching destinations for the combined, evaluated entity. This step ensures
that the destination has sufficient gpus, cores and mem to satisfy the entity's request. The maximum size of a job that
a destination can accept can be defined using the `max_accepted_cores`, `max_accepted_mem` and `max_accepted_gpus`
fields. If these are not defined, a match is assumed. In addition, all destinations that do not have scheduling tags
required by the entity are rejected, and all destinations that have scheduling tags rejected by the entity are also
rejected. Preference and acceptance is not considered at this stage, simply compatibility with available destination
based on the tag compatibility table documented later.

5. Rank
--------
After the matching destinations are short listed, they are ranked using a pluggable rank function. The default
rank function simply sorts the destinations by tags that have the most number of preferred tags, with a penalty
if preferred tags are absent. However, this default rank function can be overridden per entity, allowing a custom
rank function to be defined in python code, with arbitrary logic for picking the best match from the available
candidate destinations.

Job Dispatch Process
====================

When a typical job is dispatched, TPV follows the process below.

.. image:: ../images/job-dispatch-process.svg


#. lookup - Looks up Tool, User and Role entity definitions that match the job.
#. combine() - Combines entity requirements to create a merged entity.
#. evaluate() - Evaluates expressions in combined entity.
#. match() - Matches the combined entity requirements with a suitable destination.
#. rank() - Rank the matching destinations using a pluggable rank function.
#. choose - The entity is combined with the best matching destination and any expressions on the destination are
   evaluated, with the first non-failing match chosen (no rule failures).

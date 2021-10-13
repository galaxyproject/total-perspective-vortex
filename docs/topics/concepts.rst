Concepts and Organisation
=========================

Object types
------------

Conceptually, Vortex consists of the following types of objects.

1. Entities - An entity is anything that will be considered for scheduling
by vortex. Entities include Tools, Users, Groups, Rules and Destinations.
All entities have some common properties (id, cores, mem, env, params,
scheduling tags).

2. Scheduling Tags - Entities can have scheduling tags defined on them that determine which
entities match up, and which destination they can schedule on. Tags fall into one of four categories,
(required, preferred, accepted, rejected), ranging from indicating a requirement for a particular entity,
to indicating complete aversion.

3. Loader - The loader is responsible for loading entity definitions from a config file.
The loader will parse and validate entity definitions, including compiling python expressions,
and processing inheritance, to produce a list of entities suitable for mapping. The loader is also
capable of loading config files from multiple sources, including https urls.

4. Mapper - The mapper is responsible for routing a Galaxy job to its destination, based on the current user,
tool and job that must be scheduled. The mapper will respect the scheduling constraints expressed by the
loaded entities.


Operations
----------

When a mapper routes jobs to a destination, it does so by applying 3 basic operations on entities.

1. Inherit - The inherit operation enables an entity to inherit the properties of another entity of the same
type, and to override any required properties. While a Tool can inherit another tool, which can in-turn inherit
yet another tool, it cannot inherit a User, as it's a different entity type. It is also possibly to globally define
a `default_inherits` field, which is the entity that all entity name that all entities will inherit from should they
not have an `inherits` tag explicitly defined. Inheritance is generally processed at load time by the `Loader`,
so that there's no cost at runtime. However, the `Mapper` will process default inheritance, should the user, role
or tool that is being dispatched does not have an entry in the entities list.

When inheriting scheduling tags, if the same tag is defined by both the parent and the child, only the child's
tag will take effect. For example, if a parent defines `high-mem` as a required tag, but a child defines `high-mem`
as a preferred tag, then the tag will be treated as a preferred tag.

2. Combine - The combine operation matches up the current user, role and tool entities, and creates a combined
entity that shares all their respective preferences. The combine operation follows specific rules:

Combining gpus, cores and mem
-----------------------------
In this case, the lower of the two values are used. For example, if a user entity specific 8 cores, and a tool
requires 2 cores, then the lower value of 2 is used. An example of how this property can be used is to restrict
training users from running jobs with lower memory than the defaults when running assembly jobs.

Combining tags
-------------
When combining tags, if a role expresses a preferences for tag `training` for example, and a tool expresses a
requirement for tag `high-mem`, the combined entity would share both preferences. This can be used to route certain
roles or users to specific destinations for example.

However, if the tags are mutually exclusive, then an IncompatibleTagsException is raised. For example, if a role
expressed a preference for training, but the tool rejected tag `training`, then the job can no longer be scheduled.
If the tags are compatible, then the tag with the stronger claim takes effect. For example, if a tool requires
'high-mem` and a user prefers `high-mem`, then the combined entity will require `high-mem`. An example of using
this property would be to restrict the availability of dangerous tools only to trusted users.

Combining envs and params
-------------------------
In this case, these requirements are simply merged, with duplicate envs and params merged in the following order:
User > Role > Tool.

3. Evaluate


4. Match - The match operation can be used


Scheduling
----------

Vortex offers several mechanisms for controlling scheduling, all of which are optional.
In its simplest form, no scheduling constraints would be defined at all, in which case
the entity would schedule on the first available entity. Admins can use additional

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


Expressions
-----------

1. Python expressions
2. F-string expressions

Scheduling by tag match
------------------------
Tags can be used to model anything from compatibility with a destination, to
permissions to execute a tool. (e.g. a tool can be tagged as requiring the "restricted"
tag, and users can be tagged as rejecting the "restricted" tag by default. Then, only users
who are specifically marked as requiring, tolerating, or preferring the "restricted" tag
can execute that tool. Of course, the destination must also be marked as not rejecting the
"restricted" tag.

Scheduling by rules
-------------------


Scheduling by custom ranking functions
--------------------------------------

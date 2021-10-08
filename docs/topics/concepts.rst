Concepts and Organisation
=========================

Object types
------------

Conceptually, Vortex consists of the following types of objects.

1. Entities - An entity is anything that will be considered for scheduling
by vortex. Entities include Tools, Users, Groups, Rules and Destinations.
All entities have some common basic properties (id, cores, mem, env, params,
scheduling tags).

2. Scheduling Tags - Entities can have scheduling tags defined on them,
which determine which entities match up, and which destination they can schedule
on. Tags fall into one of four categories, ranging from indicating a preference for a particular
entity, to indicating complete aversion:

+-----------+--------------------------------------------------------------------------------------------------------+
| Tag Type  | Description                                                                                            |
+===========+========================================================================================================+
| required  | required tags must match up for scheduling to occur. For example, if a tool is marked as requiring the |
|           | `high-mem` tag, only destinations that are tagged as requiring, preferring or tolerating the           |
|           | `high-mem` tag would be considering for scheduling.                                                    |
+-----------+--------------------------------------------------------------------------------------------------------+
| preferred | preferred tags are ranked higher when scheduling decisions are made.                                   |
+-----------+--------------------------------------------------------------------------------------------------------+
| tolerated | tolerated tags can be used to indicate that a entity can match up or support another entity, even      |
|           | if not preferentially.                                                                                 |
+-----------+--------------------------------------------------------------------------------------------------------+
| rejected  | rejected tags cannot be present for scheduling to occur. For example, if a tool is marked as rejecting |
|           | the `pulsar` tag, only destinations that do not have that tag are considered for scheduling. If two    |
|           | entities have the same rejected tag, they still repel each other.                                      |
+-----------+--------------------------------------------------------------------------------------------------------+


Scheduling
----------

Vortex offers several mechanisms for controlling scheduling, all of which are optional.
In its simplest form, no scheduling constraints would be defined at all, in which case
the entity would schedule on the first available entity. Admins can use additional


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

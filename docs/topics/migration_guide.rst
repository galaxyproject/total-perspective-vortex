Migration Guide
===============

Migrating from v2.x to v3.x
---------------------------

TPV v3.0.0 introduces some potentially breaking changes that require some manual oversight. Most users will be
unaffected.

1. A new version of the TPV shared database has been released. Therefore, please update your `job_conf.yml`
   to point to https://gxy.io/tpv/db-latest.yml if you want to preserve the older behaviour of using the
   latest version. The previously use https://gxy.io/tpv/db.yml is now an alias for https://gxy.io/tpv/db-v1.yml,
   and will no longer be updated. Alternatively, you can use https://gxy.io/tpv/db-v2.yml to pin to the latest
   major version. We recommend this latter option for most users, so you can control the major version upgrade cycle.
   Refer to release notes in the TPV shared database for more information.
2. If you programmatically access TPV tags in python code, some breaking changes were made. Specifically, the
   TagSetManager class has been deprecated and the Tag class - simplified. Users using only yaml scheduling tags
   are unaffected. To create a a new Tag instance programmatically, a name is no longer required. Simply use
   pulsar_tag = Tag("mytag", TagType.REQUIRE). The TagSetManager class has been replaced with the SchedulingTags
   class. Use SchedulingTags(require=[pulsar_tag] to replace. See this `PR 136`_ for additional context.

Migrating from v1.x to v2.x
---------------------------

TPV v2.0.0 introduces a number of changes to improve simplicity by disambiguating overloaded terms and reducing code
complexity. (xref: https://github.com/galaxyproject/total-perspective-vortex/pull/58). This has resulted in the
following breaking changes.

1. `cores`, `mem` and `gpus` on destinations have been renamed to `max_accepted_cores`, `max_accepted_mem` and
   `max_accepted_gpus`. While `cores`, `mem` and `gpus` can still be defined on a destination, the result will be that
   all tools at that destination would be forcibly changed to use those core, mem and gpu values, which is probably
   not what is desired. Instead, define `max_accepted_{cores|mem|gpus}` to preserve previous behaviour.

2. In TPV 1.x, cores, mem and gpus defined on Users or Roles were used as the max_cores to allocate to a user or role.
   For example, if a tool defines cores as 4, but a user defines cores as 2, the lower of the two values would be used.
   This overloaded terminology has been disambiguated in TPV 2.x, by introducing several additional properties.
   All entities can now define: min_cores, min_mem and min_gpus as well as max_cores, max_mem and max_gpus.
   No matter how many cores a tool requests, they will be clamped between these specified min and max values.
   Therefore, in TPV 2.x, cores defined on users or roles will need to be renamed to max_cores to preserve earlier
   semantics.

3. The `runner` parameter is now required on destinations. TPV no longer reads destinations defined in job_conf.yml,
   and instead, only uses destinations defined in its own configuration files. The linter has been updated to warn you
   if the `runner` parameter is not defined.

4. The `destination_name_override` property is no longer an extra param on the destination. It is instead,
   a top-level property of a destination. The `destination_name_override` can be used to dynamically generate
   a custom name for the destination.

5. Any custom Python code that refers to scheduling tags through `entity.tags` should now use `entity.tpv_tags` on
   Tool, User, and Role entities. Destination entities now have the property `entity.tpv_dest_tags`.


.. _PR 136: https://github.com/galaxyproject/total-perspective-vortex/pull/136#issuecomment-2348846889

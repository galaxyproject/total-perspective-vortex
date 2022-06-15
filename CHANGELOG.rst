1.2.0 - Jun 15, 2022. (sha 872d200f3bfeb7356ba76bb1ee14134a50608d92)
--------------------------------------------------------------------

* vortex package and cli renamed to tpv for consistency.
* All matching entity regexes are applied, not just the first. Order of application is in the order of definition.
* When a particular entity type is matched, its definitions are cached, so that future lookups are O(1).
* Support for job resubmission handling, with integration tests for Galaxy,
* Allow destinations to be treated as regular entities, with support for rules and expressions.
* Support for global and local context variables that can be referenced in expressions.
* Improved support for complex jobs param types like dicts and lists, which are now recursively evaluated.

1.1.0 - Mar 25, 2022. (sha 0e65d9a6a16bbbfd463031677067e1af9f4dac64)
--------------------------------------------------------------------

* The "match" clause has been deprecated and renamed to "if", for easier readability of rules.
* When no default mapping for a tool exists, choose the first available destination for a simpler initial experience.
* A sample config file has been added to provide a starting point for new TPV users.

1.0.0 - Mar 07, 2022. (sha 2e082a4ec0868e03df1b902562810873421823e5)
--------------------------------------------------------------------

* Initial PyPi release of total-perspective-vortex.
* Basic usage docs and examples.
* 94% test coverage.

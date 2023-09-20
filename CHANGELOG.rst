2.3.2 - Aug 25, 2023. (sha 26ddcfb024679a8bb2d698461e7fcbfa8453c45e)
--------------------------------------------------------------------
* Fix legacy version parsing by using Galaxy's version parsing if available by @bgruening (PR #112)
* Fix E721 for type comparison by @mvdbeek (PR #113)

2.3.1 - Jul 20, 2023. (sha fc8733723fd264c06471cabc89dc32813207eff0)
--------------------------------------------------------------------
* Add two tests around destination inheritance (one fails) + fix by @cat-bro, @nuwang (PR #110)

2.3.0 - Jul 05, 2023. (sha fb5cb966e5f8e370a1566f94313146f4f3a50054)
--------------------------------------------------------------------
* fix resubmission tests by @bernt-matthias (PR #78)
* Fix issue with how default inheritance is applied to multiple files by @nuwang (PR #105)
* Fix evaluation to support resource clamping by @nuwang (PR #107)

2.2.4 - May 26, 2023. (sha 16222dffa1a8d5aac60ebe78328283b29552f8de)
--------------------------------------------------------------------
* When regex compilation fails, log what was attempting to be compiled by @natefoo (PR #97)
* Add helper for tool version equality comparison by @sanjaysrikakulam (PR #98)
* Match cores, mem and gpus whenever they are defined by @kysrpex (PR #99)
* Modify linter to flag regex errors by @nuwang  (PR #102)
* Create dependabot.yml by @nuwang (PR #103)

2.2.3 - May 04, 2023. (sha d1ac1a9553aaab19434c79e5d78f3db63647915c)
--------------------------------------------------------------------
* Add id to `mock_galaxy.User` by @kysrpex (PR #96)
* Fix dry run tool version by @cat-bro (PR #94)

2.2.2 - Apr 25, 2023. (sha 3ab3f209ffdf3117a2efcc8cf70ebbf4c57efc6f)
--------------------------------------------------------------------
* Fix formatter ordering of inherits field by @nuwang (PR #90)
* Add helpers for tool version comparison by @cat-bro (PR #91)
* Fix dry run user by @cat-bro (PR #92)
* Failing test for pulsar user scenario by @cat-bro and @nuwang (PR #93)

2.2.1 - Mar 30, 2023. (sha 060ff9e411b35a2fed444b19b61bbedfe2d2cc4d)
--------------------------------------------------------------------
* A role rule has no access to a tool's scheduling tags by @cat-bro (PR #89)

2.2.0 - Mar 22, 2023. (sha 129c65b58998661da325294fa8461f0a48fcebb0)
--------------------------------------------------------------------
* Add helper for checking entity tags within a tpv rule by @cat-bro (PR #86)
* Remove condition from tpv packaging by @nuwang (PR #87)
* refactor: remove unnecessary overrides of superclass __init__ by @nuwang (PR #88)

2.1.0 - Mar 06, 2023. (sha 0f47032a1faf416b34969fb26c332428cb74209a)
--------------------------------------------------------------------
* Automatically convert envs to string, but make sure params are preserved by @nuwang (PR #73)
* Fix indentation by @bernt-matthias in (PR #76)
* Convert env to a list compatible with Galaxy job conf by @natefoo in (PR #79)
* Fix missing app.config.is_set() in mock config for dry-run by @natefoo in (PR #80)
* Refactor and fix dryrun by @nuwang in (PR #82)
* Add initializer to handle empty sequence when reducing by @nuwang in (PR #84)

2.0.1 - Jan 11, 2023. (sha 8860c1d570ed94310f5ed57b2166390124d9bbf8)
--------------------------------------------------------------------
* create __init__.py in tpv core test folder by @bgruening (PR #70)

2.0.0 - Dec 24, 2022. (sha b593d5527bce51a7070684569dc8f19aff3d24e0)
--------------------------------------------------------------------
* Add a `tpv dry-run` subcommand by @natefoo (PR #52)
* Simplify TPV by disambiguating terms by @nuwang (PR #58)
* Additional context params by @nuwang (PR #62)
* Use only TPV defined destinations, ignoring job_conf by @nuwang (PR #63)
* Add support for abstract entities by @nuwang (PR #64)
* Add destination min accepted by @nuwang (PR #67)
* Shared rules inheritance test by @cat-bro (PR #68)
* Update github actions by @nuwang (PR #69)


1.4.1 - Nov 21, 2022. (sha 396afc17d0ba2f78e0827e6f8319246977994172)
--------------------------------------------------------------------
* Avoid deepcopying loader when cloning entities  (PR #54)
* Change remaining uses of pyyaml to ruaml.yaml (PR #51)
* Automatic PyPi deployment action (#43)


1.4.0 - Oct 12, 2022. (sha c4a49330a55f02107a9ece4ac84fd74f956f7017)
--------------------------------------------------------------------
* Added support for the execute block to change entity properties
* Added link to TPV shared database.


1.3.0 - Sep 28, 2022. (sha 3e9342622ab8bb2b6b18ef1fd32625e246eec66a)
--------------------------------------------------------------------
* Added tpv format command for prettying and ordering tpv tool lists
* Support for overriding destination name
* Fix some bugs in context variable handling
* Misc. bug fixes and refactoring


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

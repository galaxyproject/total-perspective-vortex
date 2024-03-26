[![Tests](https://github.com/galaxyproject/total-perspective-vortex/actions/workflows/tests.yaml/badge.svg)](https://github.com/galaxyproject/total-perspective-vortex/actions/workflows/tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/galaxyproject/total-perspective-vortex/badge.svg?branch=main)](https://coveralls.io/github/galaxyproject/total-perspective-vortex?branch=main)
[![Documentation Status](https://readthedocs.org/projects/total-perspective-vortex/badge/?version=latest)](http://total-perspective-vortex.readthedocs.org/en/latest/?badge=latest)

## <img src="https://raw.githubusercontent.com/galaxyproject/total-perspective-vortex/main/docs/images/tpv-logo-wide.png" width="800" height="100">

Total Perspective Vortex (TPV) provides an installable set of dynamic rules for
the [Galaxy application](https://galaxyproject.org/) that can route entities
(Tools, Users, Roles) to appropriate job destinations based on a configurable
YAML file.


### Documentation

Documentation on how to enable TPV in your Galaxy instance and configure the
relevant routing rules is available on [Read the
Docs](http://total-perspective-vortex.readthedocs.org/).

For a guided tutorial, also check out the [TPV tutorial on
GTN](https://training.galaxyproject.org/training-material/topics/admin/tutorials/job-destinations/tutorial.html).


### Installing latest released version

Once configured in the Galaxy application, TPV will be automatically installed
in the Galaxy environment. If you would like to manually install it instead, you
can do so from [PyPI](https://pypi.org/project/total-perspective-vortex/) with
the following command:

```python
pip install total-perspective-vortex
```


### Shared default resource database for tools

A shared database of reusable default resource requirements and scheduling rules
for TPV is maintained in: https://github.com/galaxyproject/tpv-shared-database/.
(This is likely something you want to take a look at - it will save a lot of
time.)

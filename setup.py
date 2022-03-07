import ast
import os
import re

import setuptools

reg = re.compile(r"__version__\s*=\s*(.+)")
with open(os.path.join("vortex", "__init__.py")) as f:
    for line in f:
        m = reg.match(line)
        if m:
            version = ast.literal_eval(m.group(1))
            break

with open("README.md", "r") as fh:
    long_description = fh.read()

REQS_FULL = [
    "cachetools>=3.1.0",
    "watchdog",
    "requests"
]

REQS_TEST = ([
    'nose',
    'galaxy-app',
    'responses',
    'tox>=2.9.1',
    'coverage>=4.4.1',
    'flake8>=3.4.1',
    'flake8-import-order>=0.13'] + REQS_FULL
)

REQS_DEV = (['sphinx', 'sphinx_rtd_theme'] + REQS_TEST)

setuptools.setup(
    name="total-perspective-vortex",
    description="A library for routing entities (jobs, users or groups) to destinations in Galaxy",
    version=version,
    author="Galaxy and GVL projects",
    author_email="help@genome.edu.au",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/galaxyproject/total-perspective-vortex",
    packages=setuptools.find_packages(),
    install_requires=REQS_FULL,
    extras_require={
        'dev': REQS_DEV,
        'test': REQS_TEST
    },
    entry_points={
        'console_scripts': [
            'vortex = vortex.core.shell:main'
        ]
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6"
    ],
    test_suite="tests"
)

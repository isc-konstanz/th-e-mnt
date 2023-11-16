#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    th-e-mnt
    ~~~~~~~~


"""
from os import path
from setuptools import setup, find_namespace_packages

here = path.abspath(path.dirname(__file__))
info = {}
with open(path.join("th_e_mnt", "_version.py")) as f:
    exec(f.read(), info)

VERSION = info['__version__']

DESCRIPTION = 'This repository provides a set of maintenance functions for several ' \
              'energy system projects of ISC Konstanz.'

# Get the long description from the README file
with open(path.join(here, 'README.md')) as f:
    README = f.read()

NAME = 'th-e-mnt'
LICENSE = 'LGPLv3'
AUTHOR = 'ISC Konstanz'
MAINTAINER_EMAIL = 'adrian.minde@isc-konstanz.de'
URL = 'https://github.com/isc-konstanz/th-e-mnt'

INSTALL_REQUIRES = [
    'corsys @ git+https://github.com/isc-konstanz/corsys.git@v0.8.4',
    'scisys @ git+https://github.com/isc-konstanz/scisys.git@v0.2.10'
]

EXTRAS_REQUIRE = {
    'mysql': ['mysql-connector-python']
}

SCRIPTS = ['bin/th-e-mnt']

PACKAGES = find_namespace_packages(include=['th_e_mnt*'])

SETUPTOOLS_KWARGS = {
    'zip_safe': False,
    'include_package_data': True
}

setup(
    name=NAME,
    version=VERSION,
    license=LICENSE,
    description=DESCRIPTION,
    long_description=README,
    author=AUTHOR,
    author_email=MAINTAINER_EMAIL,
    url=URL,
    packages=PACKAGES,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    scripts=SCRIPTS,
    **SETUPTOOLS_KWARGS
)

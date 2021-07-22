#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools
from setuptools import find_packages, setup

with open('README.md') as readme_file:
    readme = readme_file.read()

setup(
    name='plasmaflight',
    version='0.0.1',
    description="",
    long_description=readme + '\n\n',
    author="ICRAR DIA",
    author_email='callan.gray@uwa.edu.au',
    url='https://github.com/ska-telescope/ska-python-skeleton',
    #packages=setuptools.find_namespace_packages(where="", include=["ska.*"]),
    #package_dir={"": "src"},
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    license="LGPLv3 license",
    zip_safe=True,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    install_requires=[],  # FIXME: add your package's dependencies to this list
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
        # dependencies for `python setup.py build_sphinx`
        'sphinx',
        'recommonmark'
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'pytest-json-report',
        'pycodestyle',
    ],
    extras_require={
        'dev':  ['prospector[with_pyroma]', 'yapf', 'isort'],
    }
)

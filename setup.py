#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_namespace_packages, setup

with open('README.md') as readme_file:
    readme = readme_file.read()
with open('VERSION.txt') as version_file:
    version = version_file.read().strip()

setup(
    name='plasmaflight',
    version=version,
    description="Provides a plasma_store backed flight server",
    long_description=readme,
    long_description_content_type='text/markdown',
    author="ICRAR DIA",
    author_email='callan.gray@icrar.org',
    url='https://github.com/ICRAR/plasmaflight',
    packages=find_namespace_packages(where=".", include=["icrar.*"]),
    entry_points={
        'console_scripts': ['plasmaflight=icrar.plasmaflight:main']
    },
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
    install_requires=[
        'overrides',
        'pyarrow'
    ],
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
        # dependencies for `python setup.py build_sphinx`
        'sphinx',
        'recommonmark'
    ],
    tests_require=[
        'six',
        'numpy',
        'pandas',
        'pytest',
        'pytest-cov',
        'pytest-json-report',
        'pycodestyle',
    ],
    extras_require={
        'dev':  ['prospector[with_pyroma]', 'yapf', 'isort'],
    }
)

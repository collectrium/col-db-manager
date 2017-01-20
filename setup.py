#!/usr/bin/env python
import pip
from setuptools import setup, find_packages
from pip.req import parse_requirements


try:
    install_reqs = parse_requirements(
        "requirements.txt",
        session=pip.download.PipSession(),
    )
except AttributeError:
    #for pip==1.4.1
    install_reqs = parse_requirements("requirements.txt")

reqs = [str(ir.req) for ir in install_reqs if ir.req]


packages = find_packages(
    exclude=[
        '*.tests', '*.tests.*', 'tests.*', 'tests',
        '*.test', '*.test.*', 'test.*', 'test',
    ]
)

setup(
    name='col-db-manager',
    version='0.1',
    description='Common functionality for working with database',
    author='collectrium',
    author_email='support@collectrium.com',
    url='https://collectrium.com',
    packages=packages,
    install_requires=reqs,
    package_data={'': ['*.ini', '*.txt', '*.html', '*.json', '*.yml', '*.csv']},
)

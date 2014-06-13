#!/usr/bin/env python
from setuptools import setup, find_packages
import sys

if sys.platform == 'darwin':
    lxml = "lxml<2.4"
else:
    lxml = "lxml"

setup(
    name="utilities",
    version="0.0.1",
    author="Pete Aykroyd",
    author_email="aykroyd@gmail.com",
    description="utility functions",
    long_description=open("README.md").read(),
    license="Apache License 2.0",
    url="https://github.com/paykroyd/utilities",
    packages=[],
    classifiers=[
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        ],
)

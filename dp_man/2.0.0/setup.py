#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="cohesity_manager",
    version="2.0.0",
    description="A simplified interface to the Cohesity REST API",
    author="Cohesity",
    author_email="support@cohesity.com",
    url="https://github.com/cohesity/community-automation-samples",
    py_modules=["cohesity_manager"],
    install_requires=[
        "requests>=2.25.0",
    ],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: System :: Systems Administration",
    ],
    python_requires=">=3.6",
) 
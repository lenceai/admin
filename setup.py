#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name="cohesity-manager",
    version="1.0.0",
    description="Tool for managing Cohesity clusters and protection groups",
    author="Cohesity Admin",
    author_email="admin@example.com",
    py_modules=["cohesity_manager"],
    install_requires=[
        "cohesity_sdk>=1.3.0",
        "pandas>=1.0.0",
        "numpy>=1.20.0",
    ],
    entry_points={
        'console_scripts': [
            'cohesity-manager=cohesity_manager:main',
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.6",
) 
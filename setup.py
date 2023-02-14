#! /usr/bin/env python

import setuptools
from version import __version__

setuptools.setup(
    name="cdisc-rules-engine",
    version=__version__,
    description="Open source offering of the cdisc rules engine",
    author="cdisc-org",
    url="https://github.com/cdisc-org/cdisc-rules-engine",
    packages=setuptools.find_packages(exclude=["scripts", "tests"]),
    license="MIT",
    include_package_data=True,
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.5",
        "business-rules-enhanced==1.2.6",
        "python-dotenv==0.20.0",
        "cdisc-library-client==0.1.4",
        "odmlib==0.1.4",
        "xport==3.6.1",
        "redis==4.0.2",
        "openpyxl==3.0.10",
        "importlib-metadata==5.0.0",
    ],
)

import os
import imp
from setuptools import setup, find_packages


__version__ = imp.load_source(
    "hsfs.version", os.path.join("hsfs", "version.py")
).__version__


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="hsfs",
    version=__version__,
    install_requires=[
        "pyhumps",
        "requests",
        "furl",
        "boto3",
        "pandas",
        "numpy",
        "pyjks",
        "mock",
    ],
    extras_require={
        "dev": [
            "pytest",
            "flake8",
            "black"],
        "docs": [
            "mkdocs==1.1.2",
            "mkdocs-material==6.2.2",
            "mike==0.5.5",
            "keras-autodoc",
            "markdown-include"],
        "hive": ["pyhopshive[thrift]", "sqlalchemy", "PyMySQL"],
    },
    author="Logical Clocks AB",
    author_email="moritz@logicalclocks.com",
    description="HSFS: An environment independent client to interact with the Hopsworks Featurestore",
    license="Apache License 2.0",
    keywords="Hopsworks, Feature Store, Spark, Machine Learning, MLOps, DataOps",
    url="https://github.com/logicalclocks/feature-store-api",
    download_url="https://github.com/logicalclocks/feature-store-api/releases/tag/"
    + __version__,
    packages=find_packages(),
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
    ],
)

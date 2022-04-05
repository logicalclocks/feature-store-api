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
        "pyhumps==1.6.1",
        "requests",
        "furl",
        "boto3",
        "pandas>=1.2.0",
        "numpy",
        "pyjks",
        "mock",
        "avro==1.10.2",
        "sqlalchemy",
        "PyMySQL[rsa]",
    ],
    extras_require={
        "dev": ["pytest", "flake8", "black"],
        "docs": [
            "mkdocs==1.3.0",
            "mkdocs-material==8.2.8",
            "mike==1.1.2",
            "sphinx==3.5.4",
            "keras_autodoc @ git+https://git@github.com/moritzmeister/keras-autodoc@split-tags-properties",
            "markdown-include",
        ],
        "hive": [
            "pyhopshive[thrift]",
            "pyarrow",
        ],
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

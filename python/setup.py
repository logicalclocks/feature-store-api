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
    python_requires=">=3.7,<3.10",
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
        "great_expectations==0.14.3",
        "jinja2==2.11.3", # GE issue 1: great_expectations pulls in jinja 3.1.2 which causes import of great_expectations to fail
        "markupsafe==2.0.1", # GE issue 2: jinja2==2.11.3, pulls in markupsafe 2.1.0 which is not compatible with jinja2==2.11.3
        "typing_extensions>=3.7.4", # GE issue 3: missing dependency https://github.com/great-expectations/great_expectations/pull/4082/files, set to 3.7.4 to be compatible with hopsworks base environment
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
            "mkdocs-jupyter==0.21.0",
            "markdown==3.3.7",
            "pymdown-extensions",
        ],
        "hive": [
            "pyhopshive[thrift]",
            "pyarrow",
            "confluent-kafka==1.8.2",
            "fastavro==1.4.11",
        ],
        "python": [
            "pyhopshive[thrift]",
            "pyarrow",
            "confluent-kafka==1.8.2",
            "fastavro==1.4.11",
            "tqdm"
        ],
    },
    author="Hopsworks AB",
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Intended Audience :: Developers",
    ],
)

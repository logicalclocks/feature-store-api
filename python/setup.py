import os
from importlib.machinery import SourceFileLoader
from setuptools import setup, find_packages


__version__ = (
    SourceFileLoader("hsfs.version", os.path.join("hsfs", "version.py"))
    .load_module()
    .__version__
)


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="hsfs",
    version=__version__,
    python_requires=">=3.8,<3.13",
    install_requires=[
        "pyhumps==1.6.1",
        "requests",
        "furl",
        "boto3",
        "pandas<2.2.0",
        "numpy<2",
        "pyjks",
        "mock",
        "avro==1.11.3",
        "sqlalchemy<=1.4.48",  # aiomysql does not support v2 yet https://github.com/aio-libs/aiomysql/discussions/908
        "PyMySQL[rsa]",
        "great_expectations==0.15.12",
        "tzlocal",
        "fsspec",
        "retrying",
        "aiomysql",
        "opensearch-py>=1.1.0,<=2.4.2",
    ],
    extras_require={
        "dev": [
            "pytest==7.4.4",
            "pytest-mock==3.12.0",
            "flake8",
            "black",
            "pyspark==3.1.1",
            "moto[s3]==5.0.0",
        ],
        "dev-pandas1": [
            "pytest==7.4.4",
            "pytest-mock==3.12.0",
            "flake8",
            "black",
            "pyspark==3.1.1",
            "moto[s3]==5.0.0",
            "pandas<=1.5.3",
            "sqlalchemy<=1.4.48",
        ],
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
            "mkdocs-macros-plugin==0.7.0",
            "mkdocs-minify-plugin>=0.2.0",
        ],
        "hive": [
            "pyhopshive[thrift]",
            "pyarrow>=10.0",
            "confluent-kafka<=2.3.0",
            "fastavro>=1.4.11,<=1.8.4",
        ],
        "python": [
            "pyhopshive[thrift]",
            "pyarrow>=10.0",
            "confluent-kafka<=2.3.0",
            "fastavro>=1.4.11,<=1.8.4",
            "tqdm",
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
    packages=find_packages(exclude=["tests*"]),
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Intended Audience :: Developers",
    ],
)

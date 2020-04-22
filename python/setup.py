import os
import imp
from setuptools import setup, find_packages


__version__ = imp.load_source(
    'hsfs.version', os.path.join('hsfs', 'version.py')).__version__


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="hsfs",
    version=__version__,
    install_requires=[
        "pyhumps",
        "requests",
        "cryptography",
        "pyopenssl",
        "idna",
        "furl",
        "boto3",
        "pandas",
        "numpy",
        "pyhopshive[thrift]"
    ],
    extras_require={
        "dev": [
            "pytest",
            "flake8",
            "black"]
    },
    author="Moritz Meister",
    author_email="moritz@logicalclocks.com",
    description="",
    license="Apache License 2.0",
    keywords="",
    url="",
    download_url="",
    packages=find_packages(),
    long_description="",  # read('README.rst'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python :: 3",
    ],
)

import os
from setuptools import setup, find_packages
from hopsworks.version import __version__


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="hopsworks",
    version=__version__,
    install_requires=[
        "pyhumps"
    ],
    extras_require={
        "dev": [
            "pytest",
            "flake8",
            "black"],
    },
    author="Moritz Meister",
    author_email="moritz@logicalclocks.com",
    description="",
    license="GNU Affero General Public License v3",
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

# setup.py

import pathlib
import setuptools

# The directory containing this file
HERE = pathlib.Path(__file__).parent

with open(HERE / "README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="redismq-smandl",
    version="0.0.1",
    author="Steve Mandl",
    author_email="sjm34@cornell.edu",
    description="Message Queueing for Redis Streams",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/redismq",
    packages=setuptools.find_packages(exclude=("tests",)),
    install_requires=['aioredis'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)

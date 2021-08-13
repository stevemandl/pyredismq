# setup.py

import pathlib
import setuptools

# The directory containing this file
HERE = pathlib.Path(__file__).parent

with open(HERE / "README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="redismq",
    version="1.0.7",
    author="Steve Mandl",
    author_email="sjm34@cornell.edu",
    description="Message Queueing for Redis Streams",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/stevemandl/pyredismq",
    packages=setuptools.find_packages(exclude=("tests",)),
    install_requires=['aioredis'],
    extras_require={
        'dev': ['pytest', 'pytest-asyncio', 'pytest-cov', 'pytest-timeout', 'pylint']
    },
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)

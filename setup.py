# setup.py

import re
import pathlib
import setuptools

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# load in the project long description
with open(HERE / "README.md", "r") as fh:
    long_description = fh.read()

# load in the project metadata
init_py = open(HERE / "redismq" / "__init__.py").read()
metadata = dict(re.findall('__([a-z]+)__ = "([^"]+)"', init_py))

setuptools.setup(
    name="redismq",
    version=metadata["version"],
    author="Steve Mandl",
    author_email="sjm34@cornell.edu",
    description="Message Queueing for Redis Streams",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/stevemandl/pyredismq",
    packages=setuptools.find_packages(exclude=("tests",)),
    install_requires=["redis"],
    extras_require={
        "dev": ["pytest", "pytest-asyncio", "pytest-cov", "pytest-timeout", "pylint"]
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

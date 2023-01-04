from setuptools import find_packages
from skbuild import setup

__version__ = "1.3.2"

setup(
    name="momentumx",
    version=__version__,
    description="Zero-copy shared memory IPC library for building complex streaming data pipelines capable of processing large datasets",
    author="Captivation Software, LLC",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    cmake_install_dir="src/momentumx",
    extras_require={"test": ["pytest"]},
    python_requires=">=3.7",
)

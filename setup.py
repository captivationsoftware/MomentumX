from setuptools import find_packages
from skbuild import setup

__version__ = "2.2.0"

from pathlib import Path
long_description = (Path(__file__).parent / "README.md").read_text()

setup(
    name="MomentumX",
    version=__version__,
    description="Zero-copy shared memory IPC library for building complex streaming data pipelines capable of processing large datasets",
    long_description=long_description,
    long_description_content_type='text/markdown',   
    author="Captivation Software, LLC",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    cmake_install_dir="src/momentumx",
    extras_require={"test": ["pytest", "numpy"]},
    python_requires=">=3.6",
    url="https://github.com/captivationsoftware/MomentumX",
    keywords=["shm", "shared memory", "zero-copy", "numpy", "big data", "scipy", "pubsub", "pipeline"]
)

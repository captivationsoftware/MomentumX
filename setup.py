from setuptools import find_packages
from skbuild import setup

__version__ = "2.0.0"

setup(
    name="momentumx",
    version=__version__,
    description="Zero-copy shared memory IPC library for building complex streaming data pipelines capable of processing large datasets",
    author="Captivation Software, LLC",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    cmake_install_dir="src/momentumx",
    extras_require={"test": ["pytest", "numpy"]},
    python_requires=">=3.8",
    cmake_args=[
        "-DCMAKE_CXX_COMPILER_LAUNCHER=ccache"
    ]
)

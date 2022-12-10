from setuptools import setup, find_packages
from distutils.core import Extension

__version__ = '1.3.2'

libmomentumx = Extension(
    'momentumx.ext.libmomentumx', 
    sources = [
        'momentumx/ext/momentumx.cpp',
    ],
    include_dirs=['momentumx/ext/'],
    extra_compile_args=[
         '-std=c++11',
         '-g',
         '-Wall', 
         '-c', 
         '-fPIC'
    ],
    libraries=['rt']
)

setup(
    name="MomentumX",
    version=__version__,
    description="Zero-copy shared memory IPC library for building complex streaming data pipelines capable of processing large datasets",
    author="Captivation Software, LLC",
    packages=find_packages(),
    ext_modules=[libmomentumx]
)
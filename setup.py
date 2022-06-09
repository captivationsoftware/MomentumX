from setuptools import setup, find_packages
from distutils.core import Extension

__version__ = '1.1.2'

libmomentum = Extension(
    'momentum.ext.libmomentum', 
    sources = [
        'momentum/ext/momentum.cpp',
    ],
    include_dirs=['momentum/ext/'],
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
    name="momentum",
    version=__version__,
    description="Streaming / syncronous shared memory buffering",
    author="Captivation Software, LLC",
    packages=find_packages(),
    ext_modules=[libmomentum]
)
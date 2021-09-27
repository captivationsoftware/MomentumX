from setuptools import setup

setup(
    name='momentum',
    version='1.0.0',
    description='Shared Memory Data Pipelining',
    url='https://github.com/captivationsoftware/momentum',
    author='Captivation Software, LLC',
    packages=['momentum'],
    install_requires=[
      'pyzmq',
    ],
    entry_points={'console_scripts': [
        'momentum = momentum:cli',
    ]},
)

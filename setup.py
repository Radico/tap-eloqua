#!/usr/bin/env python3
from setuptools import setup

setup(
    name="tap-eloqua",
    version="0.1.1",
    description="Singer.io tap for extracting data",
    author="Simon Data",
    url="http://simondata.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_eloqua"],
    install_requires=[
        "singer-python==5.2.0",
        'requests==2.18.4',
        "pendulum==1.2.0",
        "tap-kit @ git+https://github.com/dmzobel/tap-kit.git@master",
        'ipdb==0.8.1'
    ],
    dependency_links=[
        "https://github.com/dmzobel/tap-kit/tarball/master#egg=tap-kit-0.1.1",
    ],
    entry_points="""
    [console_scripts]
    tap-eloqua=tap_eloqua:main
    """,
    packages=["tap_eloqua"],
    include_package_data=True,
)
# coding: utf8
from setuptools import setup

setup(
    name="posthub",
    install_requires=[
        "sqlalchemy",
        "basepy>=0.3.1",
        "psycopg2>=2.8.0"
    ],
    extras_require={
        'dev':[
            "pytest",
            "pytest-asyncio"
        ]
    },
)

"""
Utils used in config
"""

from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

Database = namedtuple(
    'Database',
    (
        'db',
        'default',
        'driver',
        'host',
        'password',
        'port',
        'readonly',
        'url',
        'user',
    ),
)


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = \
                super(SingletonMeta, cls).__call__(*args, **kwargs)

        return cls._instances[cls]

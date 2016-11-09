"""
Omnia config
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import re

from . import util
from .. import errors


class Config(object):
    """
    Config stores registered database configurations
    """
    __metaclass__ = util.SingletonMeta

    SUPPORTED_DRIVERS = (
        'postgresql',
        'sqlite',
    )

    _RE_DB_URL = re.compile(r'([\w\d_]+)://.*')

    def __init__(self):
        super(Config, self).__init__()

        self.__databases = {}

    def get_database(self, name=None):
        """
        Returns database parameters for given name.
        If name is None - returns database which registered as default.

        :type name: str
        :rtype: aux.Database
        """
        if name is None:
            for database in self.__databases.values():
                if database.default:
                    return database

            raise errors.DefaultDatabaseNotFound()

        database = self.__databases.get(name)

        if not database:
            raise errors.DatabaseNotFound(name)

        return database

    def register_database(
            self,
            name=None,
            url=None,
            driver=None,
            user=None,
            password=None,
            host=None,
            port=None,
            db=None,
            default=False,
            readonly=False,
    ):
        """
        Registers database
        Supports fluent interface

        :type name: str
        :type url: str | None
        :type driver: str | None
        :type user: str | None
        :type password: str | None
        :type host: str | None
        :type port: int | str | None
        :type db: str | None
        :type default: bool
        :type readonly: bool

        :rtype: Config
        """

        self.__validate_db_registration(name, default)

        database = self.__build_database(
            url,
            driver,
            user,
            password,
            host,
            port,
            db,
            default,
            readonly,
        )

        self.__databases[name] = database

        return self

    def reset(self):
        """
        Resets settings, clears databases
        """
        self.__databases.clear()

    def __validate_db_registration(self, name, default):
        if name in self.__databases:
            raise errors.DatabaseAlreadyRegistered(name)

        if default:
            for _name, _database in self.__databases.items():
                if _database.default:
                    raise errors.DefaultDatabaseAlreadyRegistered(name, _name)

        else:
            if not name:
                raise errors.DatabaseHasNoName()

    def __build_database(
            self,
            url, driver, user, password, host, port, db,
            default, readonly,
    ):
        if not bool(url) ^ any((driver, user, password, host, port, db)) \
                or not url and not all(
                    (driver, user, password, host, port, db)):
            raise errors.ConflictingConfigParams()

        if not url:
            url = '{driver}://{user}:{password}@{host}:{port}/{db}'.format(
                driver=driver,
                user=user,
                password=password,
                host=host,
                port=port,
                db=db,
            )
        else:
            match = self._RE_DB_URL.match(url)
            if not match or not match.groups():
                raise errors.InvalidDatabaseUrl(url, 'no driver specified')

            driver = match.groups()[0]

        if driver not in self.SUPPORTED_DRIVERS:
            raise errors.UnsupportedDriver(driver)

        database = util.Database(
            url=url,
            driver=driver,
            user=user,
            password=password,
            host=host,
            port=int(port) if port else None,
            db=db,
            default=bool(default),
            readonly=bool(readonly),
        )

        return database


CONFIG = Config()

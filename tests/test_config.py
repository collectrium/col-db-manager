from __future__ import absolute_import
from __future__ import unicode_literals

import unittest

from omnia import errors
from omnia.config import CONFIG


class ConfigTest(unittest.TestCase):
    longMessage = True

    def setUp(self):
        super(ConfigTest, self).setUp()

        CONFIG.reset()

    def test_register_valid_db(self):
        """verify successful registering databases with valid settings"""

        driver = 'postgresql'
        user = 'user'
        password = 'password'
        host = 'host'
        port = '1234'
        db = 'db'

        url = '{driver}://{user}:{password}@{host}:{port}/{db}'.format(
            db=db,
            driver=driver,
            host=host,
            password=password,
            port=port,
            user=user,
        )

        try:
            CONFIG.register_database('1', url=url, default=True)
        except errors.OmniaError as err:
            raise AssertionError(err)

        try:
            CONFIG.register_database(
                '2',
                driver=driver,
                user=user,
                password=password,
                host=host,
                port=port,
                db=db,
            )
        except errors.OmniaError as err:
            raise AssertionError(err)

        self.assertTrue(
            CONFIG.get_database('1'),
            'database "1" not registered',
        )
        self.assertTrue(
            CONFIG.get_database('2'),
            'database "2" not registered',
        )

        self.assertEqual(
            CONFIG.get_database('1').url,
            CONFIG.get_database('2').url,
            'database "2" url was not created properly',
        )

        default_db = CONFIG.get_database()
        self.assertTrue(default_db, 'default database was not registered')
        self.assertEqual(
            default_db,
            CONFIG.get_database('1'),
            'wrong database was registered as default'
        )

    def test_single_default(self):
        """verify that only one db is allowed to be default"""

        with self.assertRaises(errors.DefaultDatabaseNotFound):
            CONFIG.get_database()

        with self.assertRaises(errors.DatabaseNotFound):
            CONFIG.get_database('unknown db')

        with self.assertRaises(errors.InvalidDatabaseUrl):
            CONFIG.register_database('invalid url', 'invalid url')

        with self.assertRaises(errors.UnsupportedDriver):
            CONFIG.register_database('invalid driver', 'abc://')

        url = 'postgresql://'

        with self.assertRaises(errors.DatabaseHasNoName):
            CONFIG.register_database('', url)

        CONFIG \
            .register_database('1', url, default=True) \
            .register_database('2', url, default=False)

        with self.assertRaises(errors.DefaultDatabaseAlreadyRegistered) as err:
            CONFIG.register_database('3', url, default=True)

        self.assertEqual(
            str(err.exception),
            'unable to register database "3" as default:'
            ' database "1" is already registered as default',
            'wrong error message'
        )

    def test_single_name(self):
        """verify that no duplicate db names are allowed"""

        CONFIG.register_database('1', 'postgresql://1')

        with self.assertRaises(errors.ConfigError) as err:
            CONFIG.register_database('1', 'postgresql://2')

        self.assertEqual(
            str(err.exception),
            'database with name "1" is already registered'
        )

    def test_arguments(self):
        """verify database settings conflicts (url-or-params)"""

        error_msg = (
            'wrong arguments: either name'
            ' or all(driver, user, password, host, port, db)'
            ' must be provided'
        )

        # lack of necessary args

        with self.assertRaises(errors.ConfigError) as err:
            CONFIG.register_database('db')

        self.assertEqual(
            str(err.exception),
            error_msg,
            'wrong error message',
        )

        # mixed args - both url and db params

        with self.assertRaises(errors.ConfigError) as err:
            CONFIG.register_database('db', 'postgres://', 'postgresql')

        self.assertEqual(
            str(err.exception),
            error_msg,
            'wrong error message',
        )

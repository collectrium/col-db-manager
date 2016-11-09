"""
Exception classes used in Omnia
"""

from __future__ import absolute_import
from __future__ import unicode_literals


class OmniaError(Exception):
    """
    Base exception class for the whole package
    """


class ConfigError(OmniaError):
    """
    Base exception class for configuration errors
    """


class TransactionError(OmniaError):
    """
    Base exception class for transaction errors
    """


class SavepointError(OmniaError):
    """
    Base exception class for savepoint errors
    """


# --- Omnia errors

class InvalidQueryType(OmniaError):
    """
    Error issued when Omnia does not support this type of query
    """

    def __init__(self):
        super(InvalidQueryType, self).__init__('invalid query type')


# --- config errors ---

class DatabaseNotFound(ConfigError):
    """
    Error issued when requested database is not registered
    """

    def __init__(self, db_name):
        super(DatabaseNotFound, self).__init__(
            'database with name "{}" is not registered'.format(db_name)
        )


class DefaultDatabaseNotFound(ConfigError):
    """
    Error issued when no database registered as default
    """

    def __init__(self):
        super(DefaultDatabaseNotFound, self).__init__(
            'no database registered as default'
        )


class DatabaseAlreadyRegistered(ConfigError):
    """
    Error issued when database with the same name is already registered
    """

    def __init__(self, db_name):
        super(DatabaseAlreadyRegistered, self).__init__(
            'database with name "{}" is already registered'.format(db_name)
        )


class DefaultDatabaseAlreadyRegistered(ConfigError):
    """
    Error issued when default database is already registered
    """

    def __init__(self, current_db_name, default_db_name):
        super(DefaultDatabaseAlreadyRegistered, self).__init__(
            'unable to register database "{current}" as default:'
            ' database "{default}" is already registered'
            ' as default'.format(
                current=str(current_db_name),
                default=str(default_db_name))
        )


class DatabaseHasNoName(ConfigError):
    """
    Error issued when database with no name is registering as non-default
    """

    def __init__(self):
        super(DatabaseHasNoName, self).__init__(
            'database without name can not be registered as non-default'
        )


class ConflictingConfigParams(ConfigError):
    """
    Error issued when database is registering with conflicting params
    """

    def __init__(self):
        super(ConflictingConfigParams, self).__init__(
            'wrong arguments:'
            ' either name'
            ' or all(driver, user, password, host, port, db)'
            ' must be provided'
        )


class InvalidDatabaseUrl(ConfigError):
    """
    Error issued when database is registering with invalid URL
    """

    def __init__(self, url, reason=None):
        super(InvalidDatabaseUrl, self).__init__(
            'invalid database url "{}"{}'.format(
                url,
                ('' if not reason else ': {}'.format(reason)),
            )
        )


class UnsupportedDriver(ConfigError):
    """
    Error issued when database is registering with unsupported driver
    """

    def __init__(self, driver):
        super(UnsupportedDriver, self).__init__(
            'unsupported driver "{}"'.format(driver)
        )


# --- transaction errors ---


class InactiveTransaction(TransactionError):
    """
    Error issued when performing actions on inactive transaction
    """

    def __init__(self):
        super(InactiveTransaction, self).__init__('transaction is inactive')


class TransactionAlreadyOpened(TransactionError):
    """
    Error issued on attempt to open already active transaction
    """

    def __init__(self):
        super(TransactionAlreadyOpened, self).__init__(
            'transaction is already opened'
        )


class TransactionAlreadyInContext(TransactionError):
    """
    Error issued on attempt to enter transaction context twice+ times
    """

    def __init__(self):
        super(TransactionAlreadyInContext, self).__init__(
            'transaction is already in context mode'
        )


class ExplicitTransactionContextClosing(TransactionError):
    """
    Error issued on attempt to close explicitly transaction in context
    """

    def __init__(self):
        super(ExplicitTransactionContextClosing, self).__init__(
            'transaction in context mode'
            ' is not allowed to be closed explicitely'
        )


# --- savepoint errors ---

class InactiveSavepoint(SavepointError):
    """
    Error issued when performing actions on inactive savepoint
    """

    def __init__(self):
        super(InactiveSavepoint, self).__init__('savepoint is inactive')


class SavepointAlreadyOpened(SavepointError):
    """
    Error issued on attempt to open already active savepoint
    """

    def __init__(self):
        super(SavepointAlreadyOpened, self).__init__(
            'savepoint is already opened'
        )


class SavepointAlreadyInContext(SavepointError):
    """
    Error issued on attempt to enter savepoint context twice+ times
    """

    def __init__(self):
        super(SavepointAlreadyInContext, self).__init__(
            'savepoint is already in context mode'
        )


class ExplicitSavepointContextClosing(SavepointError):
    """
    Error issued on attempt to close explicitly savepoint in context
    """

    def __init__(self):
        super(ExplicitSavepointContextClosing, self).__init__(
            'savepoint in context mode'
            ' is not allowed to be closed explicitely'
        )


class SavepointHasNoParent(SavepointError):
    """
    Error issued when creating a savepoint without parent transaction/savepoint
    """

    def __init__(self):
        super(SavepointHasNoParent, self).__init__(
            'savepoint is useless outside parent transaction'
        )


class SavepointReferencesItself(SavepointError):
    """
    Error issued when creating a savepoint with itself as a parent
    """

    def __init__(self):
        super(SavepointReferencesItself, self).__init__(
            'savepoint is not allowed to be its own parent'
        )

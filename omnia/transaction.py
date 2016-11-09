"""
Transaction package
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import weakref
from collections import deque

from sqlalchemy import create_engine
from sqlalchemy import sql

from . import errors
from .config import CONFIG
from .engine import states
from .engine.savepoint import Savepoint
from .engine.session import Session


class Transaction(object):
    """
    Class which is representing a DB transaction
    Supports context interface
    Supports open()/close()/closing interface
    """

    @property
    def database(self):
        """
        The current database settings

        :rtype: omnia.config.util.Database
        """

        return self._database

    @property
    def is_context_active(self):
        """
        Tells if transaction is in context mode

        :rtype: bool
        """

        return self.is_active and bool(self.__inside_context)

    @property
    def is_active(self):
        """
        Tells if transaction is active (is opened somehow)

        :rtype: bool
        """

        active = bool(
            self.__is_connected and
            self._db_connection and
            self._db_transaction and
            self._db_connection.in_transaction() and
            self._db_transaction.is_active
        )

        if not active and self.__is_connected:
            self._cleanup()

        return active

    @property
    def session(self):
        """
        The current ORM session associated with transaction and connection

        :rtype: omnia.engine.session.Session
        """

        if not self.is_active:
            raise errors.InactiveTransaction()

        return self._orm_session_proxy

    def open(self):
        """
        Opens the transaction by connecting to DB and issuing "BEGIN" query

        :rtype: Transaction
        """

        if self.is_context_active:
            raise errors.TransactionAlreadyInContext()

        if self.is_active:
            raise errors.TransactionAlreadyOpened()

        self._deferred_queries.clear()

        self._connect()

        return self

    def close(self):
        """
        Rolls back the transaction is applicable, closes the connection
        """

        if self.is_context_active:
            raise errors.ExplicitTransactionContextClosing()

        if not self.is_active:
            return

        self.__do_rollback()
        self._cleanup()

    def commit(self):
        """
        Commits the transaction and closes connection
        Context mode: raises Committed state
        """

        if not self.is_active:
            return

        if self.is_context_active:
            raise states.Committed(self)
        else:
            self.__do_commit()
            self._cleanup()

    def defer_query(self, query):
        """
        Saves a query to be executed after transaction is closed
        Supports fluent interface

        :type query: sql.Insert | sql.Update | sql.Delete
        :rtype: Transaction
        """

        if not isinstance(query, (sql.Insert, sql.Update, sql.Delete)):
            raise errors.InvalidQueryType()

        self._deferred_queries.append(query)

        return self

    def flush(self):
        """
        Flushes data from ORM Session into DB, but does not commit transaction
        """
        if not self.is_active:
            return

        # XXX: investigate if there is db transaction flush
        self._orm_session.flush()

    def rollback(self):
        """
        Rolls changes back, closing the transaction and connection
        Context mode: raises RolledBack state
        """

        if not self.is_active:
            return

        if self.is_context_active:
            raise states.RolledBack(self)
        else:
            self.__do_rollback()
            self._cleanup()

    def savepoint(self):
        """
        Creates a savepoint under current transaction

        :rtype: Savepoint
        """

        if not self.is_active:
            raise errors.InactiveTransaction()

        return Savepoint(self)

    def __init__(self, database=None):
        self.__database_name = database
        self._database = CONFIG.get_database(database)

        self._db_engine = None
        self._db_connection = None
        self._db_transaction = None
        self._orm_session = None
        self._orm_session_proxy = None

        self._deferred_queries = deque()

        self.__is_connected = False
        self.__inside_context = False

    def _connect(self):
        """
        Starts db connection, transaction, SqlAlchemy ORM session
        """

        assert not self.__is_connected

        self._db_engine = create_engine(self._database.url)

        from sqlalchemy import event

        if self._database.driver == 'sqlite':
            @event.listens_for(self._db_engine, "connect")
            def do_connect(dbapi_connection, *args, **kwargs):
                # disable pysqlite's emitting of the BEGIN statement entirely.
                # also stops it from emitting COMMIT before any DDL.
                dbapi_connection.isolation_level = None

            @event.listens_for(self._db_engine, "begin")
            def do_begin(conn):
                # emit our own BEGIN
                conn.execute("BEGIN")

        self._db_connection = self._db_engine.connect()
        self._db_transaction = self._db_connection.begin()

        self._orm_session = Session(
            bind=self._db_connection,
            transaction=self,
        )
        self._orm_session_proxy = weakref.proxy(self._orm_session)

        self.__is_connected = True

    def _cleanup(self):
        """
        Rolls transaction back, closes the connection, deletes session
        Executes deferred queries
        """

        self._orm_session.close()
        self._db_transaction.rollback()
        self._db_connection.close()

        self._db_engine = None
        self._db_connection = None
        self._db_transaction = None
        self._orm_session = None
        self._orm_session_proxy = None

        self.__is_connected = False

        self._execute_deferred_queries()

    def _execute_deferred_queries(self):
        """
        Executes deferred queries within separate transaction
        """

        assert not self.__is_connected

        if not self._deferred_queries:
            return

        with Transaction(self.__database_name) as txn:
            while True:
                try:
                    query = self._deferred_queries.popleft()
                    txn.session.execute(query).close()
                except IndexError:
                    break

    def __enter__(self):
        self.open()

        self.__inside_context = True

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.__is_connected
        assert self.__inside_context

        if exc_val and exc_tb:
            pass  # XXX: make PyLint happy

        # context exit state is formed according to captured state exception

        if not exc_type:
            self.__do_commit()
            state_finalized = None

        elif exc_type is states.Committed:
            self.__do_commit()
            state_finalized = True

        elif exc_type is states.RolledBack:
            self.__do_rollback()
            state_finalized = True

        else:
            self.__do_rollback()
            state_finalized = False

        self._cleanup()

        self.__inside_context = False

        return state_finalized

    def __do_commit(self):
        assert self.is_active

        self._orm_session.flush()
        self._orm_session.expire_all()

        self._db_transaction.commit()

    def __do_rollback(self):
        assert self.is_active

        self._orm_session.rollback()
        self._db_transaction.rollback()

"""
Savepoint package
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import codecs
import os

from . import states
from .. import errors


class Savepoint(object):
    """
    Savepoint is a nested transaction

    Supports context interface
    Supports open/close/closing interface
    """

    @property
    def database(self):
        """
        Current database config

        :rtype: omnia.config.util.Database
        """

        return self.__parent.database

    @property
    def session(self):
        """
        Parent SqlAlchemy ORM session

        :rtype: omnia.engine.session.Session
        """

        return self.__parent.session

    def open(self):
        """
        Opens savepoint by issuing SAVEPOINT query
        """

        if self.__inside_context:
            raise errors.SavepointAlreadyInContext()

        if self.__is_opened:
            raise errors.SavepointAlreadyOpened()

        self.__parent.session.execute(
            'SAVEPOINT "{}";'.format(self.__name)
        )

        self.__is_opened = True

        return self

    def close(self):
        """
        Rolls previously opened savepoint back and performs cleanup
        """

        if self.__inside_context:
            raise errors.ExplicitSavepointContextClosing()

        if not self.__is_opened:
            return

        self.__do_rollback()

        self.__is_opened = False

    def __init__(self, parent):
        if not parent:
            raise errors.SavepointHasNoParent()

        if parent is self:
            raise errors.SavepointReferencesItself()

        self.__parent = parent
        self.__name = 'sp_{}'.format(codecs.encode(os.urandom(8), 'hex'))

        self.__is_opened = False
        self.__inside_context = False

    def savepoint(self):
        """
        Creates a new savepoint within current context
        """

        if not self.__is_opened:
            raise errors.InactiveSavepoint()

        return Savepoint(self)

    def commit(self):
        """
        Releases savepoint
        Raises Committed state when in context mode
        """

        if not self.__is_opened:
            raise errors.InactiveSavepoint()

        if self.__inside_context:
            raise states.Committed(self)
        else:
            self.__do_commit()

    def rollback(self):
        """
        Rolls savepoint back
        Raises RolledBack state when in context mode
        """

        if not self.__is_opened:
            return

        if self.__inside_context:
            raise states.RolledBack(self)
        else:
            self.__do_rollback()

    def __enter__(self):
        self.open()

        self.__inside_context = True

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb:
            pass  # XXX: make PyLint happy

        if not exc_type:
            self.__do_commit()
            state_finalized = None

        elif exc_type is states.Committed:
            self.__do_commit()
            state_finalized = bool(self == exc_val.context)

        elif exc_type is states.RolledBack:
            self.__do_rollback()
            state_finalized = bool(self == exc_val.context)

        else:
            self.__do_rollback()
            state_finalized = False

        self.__is_opened = False

        return state_finalized

    def __do_commit(self):
        self.__parent.session.flush()
        self.__parent.session.execute(
            'RELEASE SAVEPOINT "{}";'.format(self.__name)
        )

        self.__is_opened = False

    def __do_rollback(self):
        self.__parent.session.flush()
        self.__parent.session.execute(
            'ROLLBACK TO SAVEPOINT "{}";'.format(self.__name)
        )

        self.__is_opened = False

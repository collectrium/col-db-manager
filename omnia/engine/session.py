"""
Session package
"""

from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import orm


class Session(orm.Session):
    def __init__(self, *args, **kwargs):
        self.__transaction = kwargs.pop('transaction', None)

        super(Session, self).__init__(*args, **kwargs)

    def begin_nested(self):
        # TODO: return self.__context.savepoint() or deny calling this method
        super(Session, self).flush()
        super(Session, self).expire_all()
        return self

    def commit(self):
        self.flush()

    def defer_query(self, query):
        """
        Defers query to be executed after context is closed

        :type query: sqlalchemy.sql.Update | sqlalchemy.sql.Delete
        """

        if not self.__transaction:
            return

        self.__transaction.defer_query(query)

    def flush(self, *args, **kwargs):
        super(Session, self).flush(*args, **kwargs)
        self.expire_all()

    def rollback(self):
        self.expire_all()

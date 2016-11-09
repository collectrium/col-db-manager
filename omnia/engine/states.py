"""
Controlling states for context mode
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import weakref


class TransactionState(Exception):
    def __init__(self, context, *args, **kwargs):
        super(TransactionState, self).__init__(*args, **kwargs)

        self.__context = None

        if context:
            self.__context = weakref.ref(context)

    @property
    def context(self):
        """
        A context the state was raised within
        """
        return self.__context() if self.__context else None


class RolledBack(TransactionState):
    pass


class Committed(TransactionState):
    pass

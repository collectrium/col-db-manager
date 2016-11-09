from __future__ import absolute_import
from __future__ import unicode_literals

import os
import tempfile
import unittest
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from omnia.config import CONFIG

_ModelBase = declarative_base()


class TestModel(_ModelBase):
    __tablename__ = 'test_model'

    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.Text)


class TestBase(unittest.TestCase):
    longMessage = True

    def __setup_database(self):
        ts = datetime.now().strftime('%Y%m%d_%H%M')

        fd, path = tempfile.mkstemp(
            prefix='test',
            suffix=ts,
        )

        os.close(fd)

        self.__db = path

        CONFIG.register_database(
            url='sqlite:///{db}'.format(db=self.__db),
            default=True,
        )

        engine = create_engine(CONFIG.get_database().url)
        _ModelBase.metadata.create_all(engine)

    def setUp(self):
        super(TestBase, self).setUp()

        CONFIG.reset()

        self.__setup_database()

    def tearDown(self):
        CONFIG.reset()

        os.remove(self.__db)

        super(TestBase, self).tearDown()

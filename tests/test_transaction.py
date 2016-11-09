from __future__ import absolute_import
from __future__ import unicode_literals

import codecs
import os
from contextlib import closing

import sqlalchemy as sa
from sqlalchemy import exc as sa_errors
from sqlalchemy import inspect

from omnia import errors
from omnia.transaction import Transaction
from tests.base import TestBase, TestModel


class TransactionPlainTest(TestBase):
    """
    tests transaction plain workflow - open()/close()
    """

    def test_open_close(self):
        """verify that transaction may be opened once and closed many"""

        with closing(Transaction().open()) as txn:
            with self.assertRaises(errors.TransactionAlreadyOpened):
                txn.open()

        try:
            txn.close()
        except Exception:
            raise AssertionError('unable to call close() second time')

    def test_session_lifecycle(self):
        """verify that session exists only between open()/close() calls"""

        with closing(Transaction().open()) as txn:
            self.assertTrue(txn.session, 'session not started')

            obj = TestModel(id=1, value='xxx')
            txn.session.add(obj)
            txn.session.commit()

            self.assertEqual(obj.id, 1)

            i_obj = inspect(obj)

            self.assertTrue(
                i_obj.session,
                'obj is not bound to session')
            self.assertTrue(
                i_obj.persistent,
                "obj is not persistent in session")

            txn.commit()

        txn.commit()
        txn.commit()
        txn.rollback()
        txn.rollback()
        txn.flush()
        txn.flush()

        with self.assertRaises(errors.InactiveTransaction):
            txn.savepoint()

        with self.assertRaises(errors.InactiveTransaction):
            self.assertFalse(txn.session)

        self.assertIsNone(
            i_obj.session,
            'obj is still bound to session after context is closed')
        self.assertFalse(
            i_obj.persistent,
            "obj is persistent in session after closing")

    def test_db_exception_cleanup(self):
        """verify that transaction is cleaned up after internal db error"""

        with closing(Transaction().open()) as txn:
            txn.session.add(TestModel(id=1, value='xxx'))
            txn.flush()

            with self.assertRaises(sa_errors.DatabaseError):
                txn.session.add(TestModel(id=1, value='yyy'))
                txn.flush()

            with self.assertRaises(errors.InactiveTransaction):
                txn.session.flush()

    def test_context_conflict(self):
        """verify that no context mode is allowed if txn is opened"""

        with closing(Transaction().open()) as txn:
            with self.assertRaises(errors.TransactionAlreadyOpened):
                with txn:
                    pass

    def test_deferred_queries(self):
        """verify that deferred queries are executed on cleanup"""

        with closing(Transaction().open()) as txn:
            txn.session.execute(
                sa.insert(TestModel).values({
                    TestModel.id: 1,
                    TestModel.value: 'inserted value',
                })
            )

            txn.defer_query(
                sa.insert(TestModel).values({
                    TestModel.id: 1,
                    TestModel.value: 'inserted value',
                })
            ).defer_query(
                sa.update(TestModel).where(
                    TestModel.id == 1,
                ).values({
                    TestModel.value: 'updated value',
                })
            )

            txn.rollback()
            txn.close()

        with closing(Transaction().open()) as txn:
            rows = txn.session.execute(
                sa.select([TestModel.id, TestModel.value])
            ).fetchall()

            self.assertTrue(rows, 'deferred insert not invoked')
            self.assertEqual(
                len(rows), 1,
                'invalid amount of rows are present')

            row = rows[0]
            self.assertEqual(row.id, 1, 'invalid row id is stored')
            self.assertEqual(
                row.value, 'updated value',
                'deferred update not invoked')

    def test_session_actions(self):
        """verify that session commit/rollback doesn't change txn state"""

        with closing(Transaction().open()) as txn:
            txn.session.execute(
                sa.insert(TestModel).values({
                    TestModel.id: 1,
                    TestModel.value: '1',
                })
            )
            txn.session.commit()

            self.assertTrue(
                txn.is_active,
                'txn becomes inactive after commit')

        with closing(Transaction().open()) as txn:
            txn.session.execute(
                sa.insert(TestModel).values({
                    TestModel.id: 2,
                    TestModel.value: '2',
                })
            )
            txn.session.rollback()

            self.assertTrue(
                txn.is_active,
                'txn becomes inactive after rollback')

        with closing(Transaction().open()) as txn:
            rows = txn.session.execute(sa.select([TestModel.id])).fetchall()

            self.assertFalse(
                rows,
                'data are stored in db after txn closed without txn commit')


class TransactionContextTest(TestBase):
    def test_context_reentrance(self):
        """verify that transaction context cannot be re-entered"""

        with Transaction() as txn:
            with self.assertRaises(errors.TransactionAlreadyInContext):
                with txn:
                    pass

    def test_explicit_control(self):
        """verify that transaction denies open()/close() within context"""

        with Transaction() as txn:
            with self.assertRaises(errors.TransactionAlreadyInContext):
                txn.open()

            with self.assertRaises(errors.ExplicitTransactionContextClosing):
                txn.close()

    def test_session_lifecycle(self):
        """verify that session is alive only within context"""

        with Transaction() as txn:
            self.assertTrue(txn.session, 'session not started')

            obj = TestModel(id=1, value='1')
            txn.session.add(obj)
            txn.session.flush()

            self.assertEqual(obj.id, 1)

            i_obj = inspect(obj)

            self.assertTrue(
                i_obj.session,
                'obj is not bound to session')
            self.assertTrue(
                i_obj.persistent,
                "obj is not persistent in session")

        with self.assertRaises(errors.InactiveTransaction):
            self.assertFalse(txn.session)

        self.assertIsNone(
            i_obj.session,
            'obj is still bound to session after context is closed')
        self.assertFalse(
            i_obj.persistent,
            "obj is persistent in session after context is closed")

    def test_rollback_on_db_error(self):
        """verify that transaction is rolled back on db error"""

        with Transaction() as txn:
            txn.session.add(TestModel(id=1, value='1'))

        with self.assertRaises(sa_errors.DatabaseError):
            with Transaction() as txn:
                txn.session.execute(
                    sa.update(TestModel).where(
                        TestModel.id == 1,
                    ).values({
                        TestModel.value: '2',
                    })
                )

                txn.session.execute(
                    sa.insert(TestModel).values({
                        TestModel.id: 1,
                        TestModel.value: '2',
                    })
                )

        with Transaction() as txn:
            rows = txn.session.execute(
                sa.select([TestModel.id, TestModel.value])
            ).fetchall()

        self.assertTrue(rows, 'no data are stored')
        self.assertEqual(1, len(rows), 'invalid data are stored')

        row = rows[0]

        self.assertEqual(1, row.id, 'invalid data are stored')
        self.assertEqual(
            '1', row.value,
            'rollback was not triggered on db error')

    def test_rollback_on_regular_error(self):
        """verify that transaction is rolled back on regular error"""

        class TestError(Exception):
            pass

        with Transaction() as txn:
            txn.session.add(TestModel(id=1, value='1'))

        with self.assertRaises(TestError):
            with Transaction() as txn:
                txn.session.execute(
                    sa.update(TestModel).where(
                        TestModel.id == 1,
                    ).values({
                        TestModel.value: '2',
                    })
                )

                raise TestError()

        with Transaction() as txn:
            rows = txn.session.execute(
                sa.select([TestModel.id, TestModel.value])
            ).fetchall()

        self.assertTrue(rows, 'no data are stored')
        self.assertEqual(1, len(rows), 'invalid data are stored')

        row = rows[0]

        self.assertEqual(1, row.id, 'invalid data are stored')
        self.assertEqual(
            '1', row.value,
            'rollback was not triggered on regular error')

    def test_context_states(self):
        """verify that context is closed on commit()/rollback()"""

        def _verify(case, value=None):
            case += ': '

            with Transaction() as _txn:
                rows = _txn.session.execute(
                    sa.select([TestModel.id, TestModel.value])
                ).fetchall()

            if not value:
                self.assertFalse(rows, case + 'unexpected data are stored')
                return

            self.assertTrue(rows, case + 'no data are stored')
            self.assertEqual(1, len(rows), case + 'invalid data are stored')

            row = rows[0]

            self.assertEqual(1, row.id, case + 'invalid data are stored')
            self.assertEqual(
                value, row.value,
                case + 'incorrect value is stored')

        _verify('initial')

        with Transaction() as txn:
            txn.session.add(TestModel(id=1, value='x'))
            txn.rollback()
        _verify('rollback')

        with Transaction() as txn:
            txn.session.add(TestModel(id=1, value='x'))
            txn.commit()
        _verify('commit', 'x')

        with Transaction() as txn:
            obj = txn.session.query(TestModel).get(1)
            obj.value = 'y'
            txn.session.flush()
            txn.rollback()
        _verify('rollback update', 'x')

        with Transaction() as txn:
            obj = txn.session.query(TestModel).get(1)
            obj.value = 'y'
            txn.session.flush()
            txn.commit()
        _verify('commit update', 'y')

    def test_session_controls(self):
        """verify that session commit()/rollback() does not close context"""

        class TestError(Exception):
            pass

        with self.assertRaises(TestError):
            with Transaction() as txn:
                txn.session.commit()
                raise TestError()

        with self.assertRaises(TestError):
            with Transaction() as txn:
                txn.session.rollback()
                raise TestError()

    def test_deferred_queries(self):
        """verify that deferred queries are executed on context exit"""

        def _verify(case, _value):
            case += ': '

            with Transaction() as _txn:
                rows = _txn.session.execute(
                    sa.select([TestModel.id, TestModel.value])
                ).fetchall()

            self.assertTrue(rows, case + 'no data are stored')
            self.assertEqual(1, len(rows), case + 'invalid data are stored')

            row = rows[0]

            self.assertEqual(1, row.id, case + 'invalid data are stored')
            self.assertEqual(_value, row.value, case + 'update failed')

        def _defer_update(_txn):
            _rvalue = codecs.encode(os.urandom(8), 'hex')
            _query = sa.update(TestModel).where(TestModel.id == 1).values({
                TestModel.value: _rvalue,
            })
            _txn.defer_query(_query)
            _txn.session.defer_query(_query)

            return _rvalue

        class TestError(Exception):
            pass

        with Transaction() as txn:
            txn.session.add(TestModel(id=1, value=None))
            value = _defer_update(txn)
        _verify('implicit commit', value)

        with Transaction() as txn:
            value = _defer_update(txn)
            txn.commit()
        _verify('explicit commit', value)

        with Transaction() as txn:
            value = _defer_update(txn)
            txn.rollback()
        _verify('explicit rollback', value)

        with self.assertRaises(TestError):
            with Transaction() as txn:
                value = _defer_update(txn)
                raise TestError(value)
        _verify('rollback on regular error', value)

        with self.assertRaises(sa_errors.DatabaseError):
            with Transaction() as txn:
                value = _defer_update(txn)
                txn.session.execute(
                    sa.insert(TestModel).values({
                        TestModel.id: 1,
                        TestModel.value: 'xxx',
                    })
                )
        _verify('rollback on db error', value)

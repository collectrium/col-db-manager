from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import closing

import sqlalchemy as sa
from sqlalchemy import exc as sa_errors

from omnia import errors
from omnia.engine.savepoint import Savepoint
from omnia.transaction import Transaction
from tests.base import TestBase
from tests.base import TestModel


class SavepointPlainTest(TestBase):
    def test_context_denied(self):
        """verify that context entrance is disabled within open()/close()"""

        with Transaction() as txn:
            sp = txn.savepoint()

            with closing(sp.open()):
                with self.assertRaises(errors.SavepointAlreadyOpened):
                    with sp:
                        pass

    def test_open_close(self):
        """verify single open() and multiple close() calls are allowed"""

        with self.assertRaises(errors.SavepointHasNoParent):
            _ = Savepoint(None)

        with Transaction() as txn:
            with closing(txn.savepoint().open()) as sp:
                with self.assertRaises(errors.SavepointAlreadyOpened):
                    sp.open()

                sp.close()
                sp.close()

                with self.assertRaises(errors.InactiveSavepoint):
                    sp.savepoint()

                with self.assertRaises(errors.InactiveSavepoint):
                    sp.commit()

                sp.rollback()
                sp.rollback()

    def test_rollback_on_close(self):
        """verify that savepoint is rolled back on close"""

        def _get_value(_session):
            _value = _session.execute(
                sa.select([TestModel.value])
            ).scalar()

            return _value

        with Transaction() as txn:
            txn.session.execute(
                sa.insert(TestModel).values({
                    TestModel.id: 1,
                    TestModel.value: '1',
                })
            )

            with closing(txn.savepoint().open()) as sp:
                sp.session.execute(
                    sa.update(TestModel).where(TestModel.id == 1).values({
                        TestModel.value: '2',
                    })
                )

                value = _get_value(sp.session)
                self.assertEqual(value, '2', 'update failed')

            value = _get_value(txn.session)
            self.assertEqual(value, '1', 'savepoint was not rolled back')

    def test_commit_rollback_actions(self):
        """verify savepoint commit/rollback actions"""

        def _get_value(_session):
            _value = _session.execute(
                sa.select([TestModel.value])
            ).scalar()

            return _value

        with Transaction() as txn:
            value = _get_value(txn.session)
            self.assertIsNone(value, 'unexpected data is already stored')

            with closing(txn.savepoint().open()) as sp:
                sp.session.execute(sa.insert(TestModel).values({
                    TestModel.id: 1,
                    TestModel.value: '1',
                }))
                sp.commit()

            value = _get_value(txn.session)
            self.assertEqual(value, '1', 'savepoint was not released')

            with closing(txn.savepoint().open()) as sp:
                sp.session.execute(
                    sa.update(TestModel).where(
                        TestModel.id == 1,
                    ).values({
                        TestModel.value: '2',
                    })
                )
                sp.rollback()

            value = _get_value(txn.session)
            self.assertEqual(value, '1', 'savepoint was not rolled back')

    def test_nested(self):
        """verify savepoint nesting"""

        def _get_value(_session):
            _value = _session.execute(
                sa.select([TestModel.value])
            ).scalar()

            return _value

        def _set_value(_session, _value):
            _session.execute(
                sa.update(TestModel).where(TestModel.id == 1).values({
                    TestModel.value: _value,
                })
            )

        with Transaction() as txn:
            txn.session.execute(
                sa.insert(TestModel).values({
                    TestModel.id: 1,
                    TestModel.value: '1',
                })
            )

            with closing(txn.savepoint().open()) as sp_parent:
                value = _get_value(sp_parent.session)
                self.assertEqual(
                    value, '1',
                    'savepoint cannot access txn data')

                with closing(sp_parent.savepoint().open()) as sp_nested:
                    _set_value(sp_nested.session, '2')
                    value = _get_value(sp_nested.session)
                    self.assertEqual('2', value, 'update failed')

                value = _get_value(sp_parent.session)
                self.assertEqual(
                    '1', value,
                    'nested savepoint was released on close')

                with closing(sp_parent.savepoint().open()) as sp_nested:
                    _set_value(sp_nested.session, '2')
                    value = _get_value(sp_nested.session)
                    self.assertEqual('2', value, 'nested update failed')
                    sp_nested.rollback()

                value = _get_value(sp_parent.session)
                self.assertEqual(
                    '1', value,
                    'nested savepoint rollback failed')

                with closing(sp_parent.savepoint().open()) as sp_nested:
                    _set_value(sp_nested.session, '2')
                    value = _get_value(sp_nested.session)
                    self.assertEqual('2', value, 'nested update failed')
                    sp_nested.commit()

                value = _get_value(sp_parent.session)
                self.assertEqual(
                    '2', value,
                    'nested savepoint commit failed')

            value = _get_value(sp_parent.session)
            self.assertEqual(
                value, '1',
                'savepoint was released on close')


class SavepointContextTest(TestBase):
    def test_context_reentrance(self):
        """verify that context is not re-enterable"""

        with Transaction() as txn:
            with txn.savepoint() as sp:
                with self.assertRaises(errors.SavepointAlreadyInContext):
                    with sp:
                        pass

    def test_open_close(self):
        """verify that open()/close() is not allowed within context"""

        with Transaction() as txn:
            with txn.savepoint() as sp:
                with self.assertRaises(errors.SavepointAlreadyInContext):
                    sp.open()
                with self.assertRaises(errors.ExplicitSavepointContextClosing):
                    sp.close()

    def test_commit_on_exit(self):
        """verify that savepoint is released on context exit"""

        with Transaction() as txn:
            with txn.savepoint() as sp:
                sp.session.execute(
                    sa.insert(TestModel).values({
                        TestModel.id: 1,
                        TestModel.value: '1',
                    })
                )

            rows = txn.session.execute(sa.select([TestModel])).fetchall()

            self.assertTrue(rows, 'savepoint was not released')
            self.assertEqual(1, len(rows), 'invalid data')
            self.assertEqual('1', rows[0].value, 'invalid data')

    def test_commit_rollback_actions(self):
        """verify savepoint commit/rollback within context"""

        with Transaction() as txn:
            was_reached = False
            self.assertFalse(was_reached)  # XXX: pylint

            with txn.savepoint() as sp:
                sp.session.execute(
                    sa.insert(TestModel).values({
                        TestModel.id: 1,
                        TestModel.value: '2',
                    })
                )

                sp.rollback()
                was_reached = True

            self.assertFalse(
                was_reached,
                'savepoint rollback() does not close context')

            with txn.savepoint() as sp:
                sp.session.execute(
                    sa.insert(TestModel).values({
                        TestModel.id: 1,
                        TestModel.value: '1',
                    })
                )

                sp.commit()
                was_reached = True

            self.assertFalse(
                was_reached,
                'savepoint commit() does not close context')

            rows = txn.session.execute(sa.select([TestModel])).fetchall()

            self.assertTrue(rows, 'savepoint was not released')
            self.assertEqual(1, len(rows), 'invalid data')
            self.assertEqual('1', rows[0].value, 'invalid data')

    def test_db_error_cleanup(self):
        """verify that db error rolls back full transaction"""

        q_insert = sa.insert(TestModel).values({
            TestModel.id: 1,
            TestModel.value: 'xxx',
        })

        with self.assertRaises(sa_errors.DatabaseError):
            with Transaction() as txn:
                txn.session.execute(q_insert)
                with txn.savepoint() as sp:
                    sp.session.execute(q_insert)

        with Transaction() as txn:
            rows = txn.session.execute(sa.select([TestModel])).fetchall()

            self.assertFalse(rows, 'rollback was not issued on db error')

    def test_error_cleanup(self):
        """verify that exception rolls back full transaction"""

        q_insert = sa.insert(TestModel).values({
            TestModel.id: 1,
            TestModel.value: 'xxx',
        })

        class TestError(Exception):
            pass

        with self.assertRaises(TestError):
            with Transaction() as txn:
                txn.session.execute(q_insert)
                with txn.savepoint():
                    raise TestError()

        with Transaction() as txn:
            rows = txn.session.execute(sa.select([TestModel])).fetchall()

            self.assertFalse(rows, 'rollback was not issued on test error')

    def test_nested_commit(self):
        """verify that savepoint commit closes nested contexts"""

        with Transaction() as txn:
            with txn.savepoint() as sp1:
                was_reached = False
                self.assertFalse(was_reached)  # XXX: pylint

                sp1.session.add(TestModel(id=1, value='sp1'))

                with sp1.savepoint() as sp2:
                    sp2.session.add(TestModel(id=2, value='sp2'))
                    sp1.commit()

                was_reached = True

            self.assertFalse(was_reached, 'context sp1 was not closed')

            rows = txn.session.execute(sa.select([TestModel])).fetchall()

            self.assertTrue(rows, 'no data inserted')
            self.assertEqual(2, len(rows), 'not enough data inserted')
            self.assertEqual(
                {'sp1', 'sp2'},
                set(r.value for r in rows),
                'invalid data is stored'
            )

    def test_nested_rollback(self):
        """verify that savepoint rollback closes nested contexts"""

        with Transaction() as txn:
            with txn.savepoint() as sp1:
                was_reached = False
                self.assertFalse(was_reached)  # XXX: pylint

                sp1.session.add(TestModel(id=1, value='sp1'))

                with sp1.savepoint() as sp2:
                    sp2.session.add(TestModel(id=2, value='sp2'))
                    sp1.rollback()

                was_reached = True

            self.assertFalse(was_reached, 'context sp1 was not closed')

            rows = txn.session.execute(sa.select([TestModel])).fetchall()

            self.assertFalse(rows, 'data suddenly inserted')

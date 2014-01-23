"""
Test model API
"""
import copy
import six
from pynamodb.constants import ITEM, STRING_SHORT
from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, BinaryAttribute,
    UnicodeSetAttribute, NumberSetAttribute, BinarySetAttribute)
from unittest import TestCase
from .response import HttpOK
from .data import (
    MODEL_TABLE_DATA, GET_MODEL_ITEM_DATA, SIMPLE_MODEL_TABLE_DATA,
    BATCH_GET_ITEMS, SIMPLE_BATCH_GET_ITEMS)

# Py2/3
try:
    from unittest.mock import patch
    from unittest.mock import MagicMock
except ImportError:
    from mock import patch
    from mock import MagicMock

PATCH_METHOD = 'botocore.operation.Operation.call'


class SimpleUserModel(Model):
    """
    A hash key only model
    """
    table_name = 'SimpleModel'
    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    numbers = NumberSetAttribute()
    aliases = UnicodeSetAttribute()
    icons = BinarySetAttribute()


class UserModel(Model):
    """
    A testing model
    """
    table_name = 'UserModel'
    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)
    picture = BinaryAttribute(null=True)
    zip_code = NumberAttribute(null=True)
    email = UnicodeAttribute(default='needs_email')
    callable_field = NumberAttribute(default=lambda: 42)


class ModelTestCase(TestCase):
    """
    Tests for the models API
    """

    def assert_dict_lists_equal(self, list1, list2):
        """
        Compares two lists of dictionariess
        """
        for d1_item in list1:
            found = False
            for d2_item in list2:
                if d2_item.items() == d1_item.items():
                    found = True
            if not found:
                if six.PY3:
                    #TODO WTF python2?
                    raise AssertionError("Values not equal: {0} {1}".format(d1_item, list2))

    def test_create_model(self):
        """
        Model.create_table
        """
        self.maxDiff = None
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK, MODEL_TABLE_DATA
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2)

    def test_model_attrs(self):
        """
        Model()
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(MODEL_TABLE_DATA), MODEL_TABLE_DATA
            item = UserModel('foo', 'bar')
            self.assertEqual(item.email, 'needs_email')
            self.assertEqual(item.callable_field, 42)
            self.assertEqual(repr(item), '{0}<{1}, {2}>'.format(UserModel.table_name, item.user_name, item.user_id))
            self.assertEqual(repr(UserModel.meta()), 'MetaTable<{0}>'.format('Thread'))

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(SIMPLE_MODEL_TABLE_DATA), SIMPLE_MODEL_TABLE_DATA
            item = SimpleUserModel('foo')
            self.assertEqual(repr(item), '{0}<{1}>'.format(SimpleUserModel.table_name, item.user_name))
            self.assertRaises(ValueError, item.save)

        self.assertRaises(ValueError, UserModel.from_raw_data, None)

    def test_update(self):
        """
        Model.update
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            item = UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(GET_MODEL_ITEM_DATA), GET_MODEL_ITEM_DATA
            item.update()
            self.assertEqual(
                item.user_name,
                GET_MODEL_ITEM_DATA.get(ITEM).get('user_name').get(STRING_SHORT))

    def test_delete(self):
        """
        Model.delete
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            item = UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            item.delete()
            params = {
                'key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'table_name': 'UserModel'
            }
            args = req.call_args[1]
            self.assertEqual(args, params)

    def test_save(self):
        """
        Model.save
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            item = UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            item.save()
            args = req.call_args[1]
            params = {
                'item': {
                    'callable_field': {
                        'N': 42
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    }
                },
                'table_name': 'UserModel'
            }

            self.assertEqual(args, params)

    def test_scan(self):
        """
        Model.scan
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            for item, attrs in zip(UserModel.scan(), items):
                self.assertEqual(item.user_id, attrs['user_id'].get(STRING_SHORT))

    def test_get(self):
        """
        Model.get
        """
        def fake_dynamodb(*args, **kwargs):
            if kwargs == {'table_name': UserModel.table_name}:
                return HttpOK(MODEL_TABLE_DATA), MODEL_TABLE_DATA
            elif kwargs == {
                'table_name': 'UserModel',
                'key': {'user_name': {'S': 'foo'},
                        'user_id': {'S': 'bar'}}, 'consistent_read': False}:
                return HttpOK(GET_MODEL_ITEM_DATA), GET_MODEL_ITEM_DATA
            return HttpOK(), MODEL_TABLE_DATA

        FakeDB = MagicMock()
        FakeDB.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=FakeDB) as req:
            item = UserModel.get(
                'foo',
                'bar'
            )
            self.assertEqual(item.get_keys(), {'user_id': 'bar', 'user_name': 'foo'})
            params = {
                'consistent_read': False,
                'key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'table_name': 'UserModel'
            }
            self.assertEqual(req.call_args[1], params)
            item.zip_code = 88030
            self.assertEqual(item.zip_code, 88030)

    def test_batch_get(self):
        """
        Model.batch_get
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), SIMPLE_MODEL_TABLE_DATA
            SimpleUserModel('foo')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), SIMPLE_BATCH_GET_ITEMS
            item_keys = ['hash-{0}'.format(x) for x in range(10)]
            SimpleUserModel.batch_get(item_keys)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            item_keys = [('hash-{0}'.format(x), '{0}'.format(x)) for x in range(10)]
            req.return_value = HttpOK(), BATCH_GET_ITEMS
            for item in UserModel.batch_get(item_keys):
                self.assertIsNotNone(item)
            params = {
                'request_items': {
                    'UserModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-0'}, 'user_id': {'S': '0'}},
                            {'user_name': {'S': 'hash-1'}, 'user_id': {'S': '1'}},
                            {'user_name': {'S': 'hash-2'}, 'user_id': {'S': '2'}},
                            {'user_name': {'S': 'hash-3'}, 'user_id': {'S': '3'}},
                            {'user_name': {'S': 'hash-4'}, 'user_id': {'S': '4'}},
                            {'user_name': {'S': 'hash-5'}, 'user_id': {'S': '5'}},
                            {'user_name': {'S': 'hash-6'}, 'user_id': {'S': '6'}},
                            {'user_name': {'S': 'hash-7'}, 'user_id': {'S': '7'}},
                            {'user_name': {'S': 'hash-8'}, 'user_id': {'S': '8'}},
                            {'user_name': {'S': 'hash-9'}, 'user_id': {'S': '9'}}
                        ]
                    }
                }
            }
            args = req.call_args[1]
            self.assertTrue('request_items' in params)
            self.assertTrue('UserModel' in params['request_items'])
            self.assertTrue('Keys' in params['request_items']['UserModel'])
            self.assert_dict_lists_equal(
                params['request_items']['UserModel']['Keys'],
                args['request_items']['UserModel']['Keys'],
            )

    def test_batch_write(self):
        """
        Model.batch_write
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None

            with UserModel.batch_write(auto_commit=False) as batch:
                items = [UserModel('hash-{0}'.format(x), '{0}'.format(x)) for x in range(25)]
                for item in items:
                    batch.save(item)
                self.assertRaises(ValueError, batch.save, UserModel('asdf', '1234'))

            with UserModel.batch_write() as batch:
                items = [UserModel('hash-{0}'.format(x), '{0}'.format(x)) for x in range(30)]
                for item in items:
                    batch.delete(item)

            with UserModel.batch_write() as batch:
                items = [UserModel('hash-{0}'.format(x), '{0}'.format(x)) for x in range(30)]
                for item in items:
                    batch.save(item)

"""
Test model API
"""
import copy
import six
from pynamodb.connection.exceptions import TableError
from pynamodb.types import RANGE
from pynamodb.constants import (
    ITEM, STRING_SHORT, ALL, KEYS_ONLY, INCLUDE
)
from pynamodb.models import Model
from pynamodb.indexes import (
    GlobalSecondaryIndex, LocalSecondaryIndex, AllProjection,
    IncludeProjection, KeysOnlyProjection, Index
)
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, BinaryAttribute,
    UnicodeSetAttribute, NumberSetAttribute, BinarySetAttribute)
from unittest import TestCase
from .response import HttpOK, HttpBadRequest
from .data import (
    MODEL_TABLE_DATA, GET_MODEL_ITEM_DATA, SIMPLE_MODEL_TABLE_DATA,
    BATCH_GET_ITEMS, SIMPLE_BATCH_GET_ITEMS)

# Py2/3
if six.PY3:
    from unittest.mock import patch
    from unittest.mock import MagicMock
else:
    from mock import patch
    from mock import MagicMock

PATCH_METHOD = 'botocore.operation.Operation.call'


class EmailIndex(GlobalSecondaryIndex):
    """
    A global secondary index for email addresses
    """
    read_capacity_units = 2
    write_capacity_units = 1
    projection = AllProjection()
    email = UnicodeAttribute(hash_key=True)
    numbers = NumberSetAttribute(range_key=True)


class LocalEmailIndex(LocalSecondaryIndex):
    """
    A global secondary index for email addresses
    """
    read_capacity_units = 2
    write_capacity_units = 1
    projection = AllProjection()
    email = UnicodeAttribute(hash_key=True)
    numbers = NumberSetAttribute(range_key=True)


class IndexedModel(Model):
    """
    A model with an index
    """
    table_name = 'SimpleModel'
    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    email_index = EmailIndex()
    numbers = NumberSetAttribute()
    aliases = UnicodeSetAttribute()
    icons = BinarySetAttribute()


class LocalIndexedModel(Model):
    """
    A model with an index
    """
    table_name = 'SimpleModel'
    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    email_index = LocalEmailIndex()
    numbers = NumberSetAttribute()
    aliases = UnicodeSetAttribute()
    icons = BinarySetAttribute()


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
        scope_args = {'count': 0}

        def fake_dynamodb(obj, **kwargs):
            if kwargs == {'table_name': UserModel.table_name}:
                if scope_args['count'] == 0:
                    return HttpBadRequest(), {}
                elif scope_args['count'] == 1:
                    data = copy.copy(MODEL_TABLE_DATA)
                    data['TableStatus'] = 'Creating'
                    scope_args['count'] += 1
                    return HttpOK(content=data), data
                else:
                    return MODEL_TABLE_DATA
            else:
                return HttpOK(content={}), {}

        FakeDB = MagicMock()
        FakeDB.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=FakeDB) as req:
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK, MODEL_TABLE_DATA
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2)

        def fake_wait(obj, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                return HttpBadRequest(), {}
            elif scope_args['count'] == 1 or scope_args['count'] == 2:
                data = copy.deepcopy(MODEL_TABLE_DATA)
                data['Table']['TableStatus'] = 'Creating'
                scope_args['count'] += 1
                return HttpOK(content=data), data
            else:
                return HttpOK(MODEL_TABLE_DATA), MODEL_TABLE_DATA

        FakeWait = MagicMock()
        FakeWait.side_effect = fake_wait

        scope_args = {'count': 0}
        with patch(PATCH_METHOD, new=FakeWait) as req:
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2, wait=True)

        def bad_server(obj, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                return HttpBadRequest(), {}
            elif scope_args['count'] == 1 or scope_args['count'] == 2:
                return HttpBadRequest(), {}

        BadServer = MagicMock()
        BadServer.side_effect = bad_server

        scope_args = {'count': 0}
        with patch(PATCH_METHOD, new=BadServer) as req:
            self.assertRaises(TableError, UserModel.create_table, read_capacity_units=2, write_capacity_units=2, wait=True)

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

    def test_refresh(self):
        """
        Model.refresh
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            item = UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(GET_MODEL_ITEM_DATA), GET_MODEL_ITEM_DATA
            item.refresh()
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
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'table_name': 'UserModel'
            }

            self.assertEqual(args, params)

    def test_query(self):
        """
        Model.query
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
            queried = []
            for item in UserModel.query('foo', user_id__between=['id-1', 'id-3']):
                queried.append(item.serialize().get(RANGE))
            self.assertListEqual(
                [item.get('user_id').get(STRING_SHORT) for item in items],
                queried
            )

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []
            for item in UserModel.query('foo', user_id__gt='id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []
            for item in UserModel.query('foo', user_id__lt='id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []
            for item in UserModel.query('foo', user_id__ge='id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []
            for item in UserModel.query('foo', user_id__le='id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []
            for item in UserModel.query('foo', user_id__eq='id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []
            for item in UserModel.query('foo', user_id__begins_with='id'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []
            for item in UserModel.query('foo'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

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
            scanned_items = []
            for item in UserModel.scan():
                scanned_items.append(item.serialize().get(RANGE))
            self.assertListEqual(
                [item.get('user_id').get(STRING_SHORT) for item in items],
                scanned_items
            )

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

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
            self.assertIsNone(UserModel.get('foo', 'bar'))

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
            for item in SimpleUserModel.batch_get(item_keys):
                self.assertIsNotNone(item)

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
                    batch.delete(item)
                self.assertRaises(ValueError, batch.save, UserModel('asdf', '1234'))

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

    def test_global_index(self):
        """
        Models.GlobalSecondaryIndex
        """
        self.assertIsNotNone(IndexedModel.email_index.hash_key_attribute())

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            IndexedModel('foo', 'bar')

        scope_args = {'count': 0}

        def fake_dynamodb(obj, **kwargs):
            if kwargs == {'table_name': UserModel.table_name}:
                if scope_args['count'] == 0:
                    return HttpBadRequest(), {}
                elif scope_args['count'] == 1:
                    data = copy.copy(MODEL_TABLE_DATA)
                    data['TableStatus'] = 'Creating'
                    scope_args['count'] += 1
                    return HttpOK(content=data), data
                else:
                    return MODEL_TABLE_DATA
            else:
                return HttpOK(content={}), {}

        FakeDB = MagicMock()
        FakeDB.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=FakeDB) as req:
            IndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = {
                'attribute_definitions': [
                    {'attribute_name': 'email', 'attribute_type': 'S'},
                    {'attribute_name': 'numbers', 'attribute_type': 'NS'}
                ],
                'key_schema': [
                    {'AttributeName': 'numbers', 'KeyType': 'RANGE'},
                    {'AttributeName': 'email', 'KeyType': 'HASH'}
                ]
            }
            schema = IndexedModel.email_index.schema()
            args = req.call_args[1]
            self.assertEqual(args['global_secondary_indexes'][0]['ProvisionedThroughput'], {'ReadCapacityUnits': 2, 'WriteCapacityUnits': 1})
            self.assert_dict_lists_equal(schema['key_schema'], params['key_schema'])
            self.assert_dict_lists_equal(schema['attribute_definitions'], params['attribute_definitions'])

    def test_local_index(self):
        """
        Models.LocalSecondaryIndex
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            LocalIndexedModel('foo', 'bar')

        scope_args = {'count': 0}

        def fake_dynamodb(obj, **kwargs):
            if kwargs == {'table_name': UserModel.table_name}:
                if scope_args['count'] == 0:
                    return HttpBadRequest(), {}
                elif scope_args['count'] == 1:
                    data = copy.copy(MODEL_TABLE_DATA)
                    data['TableStatus'] = 'Creating'
                    scope_args['count'] += 1
                    return HttpOK(content=data), data
                else:
                    return MODEL_TABLE_DATA
            else:
                return HttpOK(content={}), {}

        FakeDB = MagicMock()
        FakeDB.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=FakeDB) as req:
            LocalIndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = {
                'attribute_definitions': [
                    {
                        'attribute_name': 'email', 'attribute_type': 'S'
                    },
                    {
                        'attribute_name': 'numbers',
                        'attribute_type': 'NS'
                    }
                ],
                'key_schema': [
                    {
                        'AttributeName': 'email', 'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'numbers', 'KeyType': 'RANGE'
                    }
                ]
            }
            schema = LocalIndexedModel.email_index.schema()
            args = req.call_args[1]
            self.assertTrue('ProvisionedThroughput' not in args['local_secondary_indexes'][0])
            self.assert_dict_lists_equal(schema['key_schema'], params['key_schema'])
            self.assert_dict_lists_equal(params['attribute_definitions'], schema['attribute_definitions'])

    def test_projections(self):
        """
        Models.Projection
        """
        projection = AllProjection()
        self.assertEqual(projection.projection_type, ALL)

        projection = KeysOnlyProjection()
        self.assertEqual(projection.projection_type, KEYS_ONLY)

        projection = IncludeProjection(non_attr_keys=['foo', 'bar'])
        self.assertEqual(projection.projection_type, INCLUDE)
        self.assertEqual(projection.non_key_attributes, ['foo', 'bar'])

        self.assertRaises(ValueError, IncludeProjection, None)

        class BadIndex(Index):
            pass

        self.assertRaises(ValueError, BadIndex)

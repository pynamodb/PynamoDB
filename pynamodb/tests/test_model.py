"""
Test model API
"""
import random
import json
import six
import copy
from datetime import datetime
from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.compat import OrderedDict
from pynamodb.throttle import Throttle
from pynamodb.connection.util import pythonic
from pynamodb.exceptions import TableError
from pynamodb.types import RANGE
from pynamodb.constants import (
    ITEM, STRING_SHORT, ALL, KEYS_ONLY, INCLUDE, REQUEST_ITEMS, UNPROCESSED_KEYS,
    RESPONSES, KEYS, ITEMS, LAST_EVALUATED_KEY, EXCLUSIVE_START_KEY, ATTRIBUTES, BINARY_SHORT
)
from pynamodb.models import Model
from pynamodb.indexes import (
    GlobalSecondaryIndex, LocalSecondaryIndex, AllProjection,
    IncludeProjection, KeysOnlyProjection, Index
)
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, BinaryAttribute, UTCDateTimeAttribute,
    UnicodeSetAttribute, NumberSetAttribute, BinarySetAttribute)
from .response import HttpOK, HttpBadRequest
from .data import (
    MODEL_TABLE_DATA, GET_MODEL_ITEM_DATA, SIMPLE_MODEL_TABLE_DATA,
    BATCH_GET_ITEMS, SIMPLE_BATCH_GET_ITEMS, COMPLEX_TABLE_DATA,
    COMPLEX_ITEM_DATA, INDEX_TABLE_DATA, LOCAL_INDEX_TABLE_DATA,
    CUSTOM_ATTR_NAME_INDEX_TABLE_DATA, CUSTOM_ATTR_NAME_ITEM_DATA,
    BINARY_ATTR_DATA, SERIALIZED_TABLE_DATA
)

if six.PY3:
    from unittest.mock import patch, MagicMock
else:
    from mock import patch, MagicMock

PATCH_METHOD = 'botocore.operation.Operation.call'


class GamePlayerOpponentIndex(LocalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GamePlayerOpponentIndex"
        host = "http://localhost:8000"
        projection = AllProjection()

    player_id = UnicodeAttribute(hash_key=True)
    winner_id = UnicodeAttribute(range_key=True)


class GameOpponentTimeIndex(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameOpponentTimeIndex"
        host = "http://localhost:8000"
        projection = AllProjection()

    winner_id = UnicodeAttribute(hash_key=True)
    created_time = UnicodeAttribute(range_key=True)


class GameModel(Model):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameModel"
        host = "http://localhost:8000"

    player_id = UnicodeAttribute(hash_key=True)
    created_time = UTCDateTimeAttribute(range_key=True)
    winner_id = UnicodeAttribute()
    loser_id = UnicodeAttribute(null=True)

    player_opponent_index = GamePlayerOpponentIndex()
    opponent_time_index = GameOpponentTimeIndex()


class OldStyleModel(Model):
    _table_name = 'IndexedModel'
    user_name = UnicodeAttribute(hash_key=True)


class EmailIndex(GlobalSecondaryIndex):
    """
    A global secondary index for email addresses
    """
    class Meta:
        index_name = 'custom_idx_name'
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()
    email = UnicodeAttribute(hash_key=True)
    alt_numbers = NumberSetAttribute(range_key=True, attr_name='numbers')


class LocalEmailIndex(LocalSecondaryIndex):
    """
    A global secondary index for email addresses
    """
    class Meta:
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()
    email = UnicodeAttribute(hash_key=True)
    numbers = NumberSetAttribute(range_key=True)


class NonKeyAttrIndex(LocalSecondaryIndex):
    class Meta:
        index_name = "non_key_idx"
        read_capacity_units = 2
        write_capacity_units = 1
        projection = IncludeProjection(non_attr_keys=['numbers'])
    email = UnicodeAttribute(hash_key=True)
    numbers = NumberSetAttribute(range_key=True)


class IndexedModel(Model):
    """
    A model with an index
    """
    class Meta:
        table_name = 'IndexedModel'
    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    email_index = EmailIndex()
    include_index = NonKeyAttrIndex()
    numbers = NumberSetAttribute()
    aliases = UnicodeSetAttribute()
    icons = BinarySetAttribute()


class LocalIndexedModel(Model):
    """
    A model with an index
    """
    class Meta:
        table_name = 'LocalIndexedModel'
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
    class Meta:
        table_name = 'SimpleModel'
    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    numbers = NumberSetAttribute()
    custom_aliases = UnicodeSetAttribute(attr_name='aliases')
    icons = BinarySetAttribute()
    views = NumberAttribute(null=True)


class ThrottledUserModel(Model):
    """
    A testing model
    """
    class Meta:
        table_name = 'UserModel'
    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)
    throttle = Throttle('50')


class CustomAttrIndex(LocalSecondaryIndex):
    class Meta:
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()
    overidden_uid = UnicodeAttribute(hash_key=True, attr_name='user_id')


class CustomAttrNameModel(Model):
    """
    A testing model
    """
    class Meta:
        table_name = 'CustomAttrModel'
    overidden_user_name = UnicodeAttribute(hash_key=True, attr_name='user_name')
    overidden_user_id = UnicodeAttribute(range_key=True, attr_name='user_id')
    overidden_attr = UnicodeAttribute(attr_name='foo_attr', null=True)
    uid_index = CustomAttrIndex()


class UserModel(Model):
    """
    A testing model
    """
    class Meta:
        table_name = 'UserModel'
        read_capacity_units = 25
        write_capacity_units = 25
    custom_user_name = UnicodeAttribute(hash_key=True, attr_name='user_name')
    user_id = UnicodeAttribute(range_key=True)
    picture = BinaryAttribute(null=True)
    zip_code = NumberAttribute(null=True)
    email = UnicodeAttribute(default='needs_email')
    callable_field = NumberAttribute(default=lambda: 42)


class HostSpecificModel(Model):
    """
    A testing model
    """
    class Meta:
        host = 'http://localhost'
        table_name = 'RegionSpecificModel'
    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)


class RegionSpecificModel(Model):
    """
    A testing model
    """
    class Meta:
        region = 'us-west-1'
        table_name = 'RegionSpecificModel'
    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)


class ComplexKeyModel(Model):
    """
    This model has a key that must be serialized/deserialized properly
    """
    class Meta:
        table_name = 'ComplexKey'
    name = UnicodeAttribute(hash_key=True)
    date_created = UTCDateTimeAttribute(default=datetime.utcnow)


class ModelTestCase(TestCase):
    """
    Tests for the models API
    """

    def assert_dict_lists_equal(self, list1, list2):
        """
        Compares two lists of dictionaries
        """
        for d1_item in list1:
            found = False
            for d2_item in list2:
                if d2_item.items() == d1_item.items():
                    found = True
            if not found:
                if six.PY3:
                    # TODO WTF python2?
                    raise AssertionError("Values not equal: {0} {1}".format(d1_item, list2))
        if len(list1) != len(list2):
            raise AssertionError("Values not equal: {0} {1}".format(list1, list2))

    def test_create_model(self):
        """
        Model.create_table
        """
        self.maxDiff = None
        scope_args = {'count': 0}

        def fake_dynamodb(obj, **kwargs):
            if kwargs == {'table_name': UserModel.Meta.table_name}:
                if scope_args['count'] == 0:
                    return HttpBadRequest(), {}
                else:
                    return MODEL_TABLE_DATA
            else:
                return HttpOK(content={}), {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as outer:
            with patch("pynamodb.connection.TableConnection.describe_table") as req:
                req.return_value = None
                with self.assertRaises(ValueError):
                    UserModel.create_table(read_capacity_units=2, write_capacity_units=2, wait=True)

        with patch(PATCH_METHOD, new=fake_db) as req:
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2)

        # Test for default region
        self.assertEqual(UserModel.Meta.region, 'us-east-1')
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK, MODEL_TABLE_DATA
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2)
            # The default region is us-east-1
            self.assertEqual(req.call_args[0][0].region_name, 'us-east-1')

        # A table with a specified region
        self.assertEqual(RegionSpecificModel.Meta.region, 'us-west-1')
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK, MODEL_TABLE_DATA
            RegionSpecificModel.create_table(read_capacity_units=2, write_capacity_units=2)
            self.assertEqual(req.call_args[0][0].region_name, 'us-west-1')

        # A table with a specified host
        self.assertEqual(HostSpecificModel.Meta.host, 'http://localhost')
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK, MODEL_TABLE_DATA
            HostSpecificModel.create_table(read_capacity_units=2, write_capacity_units=2)
            self.assertEqual(req.call_args[0][0].host, 'http://localhost')

        # A table with a specified capacity
        self.assertEqual(UserModel.Meta.read_capacity_units, 25)
        self.assertEqual(UserModel.Meta.write_capacity_units, 25)

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

        mock_wait = MagicMock()
        mock_wait.side_effect = fake_wait

        scope_args = {'count': 0}
        with patch(PATCH_METHOD, new=mock_wait) as req:
            UserModel.create_table(wait=True)
            params = {
                'attribute_definitions': [
                    {
                        'AttributeName': 'user_name',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'user_id',
                        'AttributeType': 'S'
                    }
                ],
                'key_schema': [
                    {
                        'AttributeName': 'user_name',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'user_id',
                        'KeyType': 'RANGE'
                    }
                ],
                'provisioned_throughput': {
                    'ReadCapacityUnits': 25, 'WriteCapacityUnits': 25
                },
                'table_name': 'UserModel'
            }

            self.assertEqual(req.call_args_list[1][1], params)

        def bad_server(obj, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                return HttpBadRequest(), {}
            elif scope_args['count'] == 1 or scope_args['count'] == 2:
                return HttpBadRequest(), {}

        bad_mock_server = MagicMock()
        bad_mock_server.side_effect = bad_server

        scope_args = {'count': 0}
        with patch(PATCH_METHOD, new=bad_mock_server) as req:
            self.assertRaises(
                TableError,
                UserModel.create_table,
                read_capacity_units=2,
                write_capacity_units=2,
                wait=True
            )

    def test_model_attrs(self):
        """
        Model()
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(MODEL_TABLE_DATA), MODEL_TABLE_DATA
            item = UserModel('foo', 'bar')
            self.assertEqual(item.email, 'needs_email')
            self.assertEqual(item.callable_field, 42)
            self.assertEqual(
                repr(item), '{0}<{1}, {2}>'.format(UserModel.Meta.table_name, item.custom_user_name, item.user_id)
            )
            self.assertEqual(repr(UserModel._get_meta_data()), 'MetaTable<{0}>'.format('Thread'))

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(SIMPLE_MODEL_TABLE_DATA), SIMPLE_MODEL_TABLE_DATA
            item = SimpleUserModel('foo')
            self.assertEqual(repr(item), '{0}<{1}>'.format(SimpleUserModel.Meta.table_name, item.user_name))
            self.assertRaises(ValueError, item.save)

        self.assertRaises(ValueError, UserModel.from_raw_data, None)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), CUSTOM_ATTR_NAME_INDEX_TABLE_DATA
            item = CustomAttrNameModel('foo', 'bar', overidden_attr='test')
            self.assertEqual(item.overidden_attr, 'test')
            self.assertTrue(not hasattr(item, 'foo_attr'))

    def test_overidden_defaults(self):
        """
        Custom attribute names
        """
        schema = CustomAttrNameModel._get_schema()
        correct_schema = {
            'key_schema': [
                {'key_type': 'HASH', 'attribute_name': 'user_name'},
                {'key_type': 'RANGE', 'attribute_name': 'user_id'}
            ],
            'attribute_definitions': [
                {'attribute_type': 'S', 'attribute_name': 'user_name'},
                {'attribute_type': 'S', 'attribute_name': 'user_id'}
            ]
        }
        self.assert_dict_lists_equal(correct_schema['key_schema'], schema['key_schema'])
        self.assert_dict_lists_equal(correct_schema['attribute_definitions'], schema['attribute_definitions'])

    def test_refresh(self):
        """
        Model.refresh
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            item = UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            self.assertRaises(item.DoesNotExist, item.refresh)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(GET_MODEL_ITEM_DATA), GET_MODEL_ITEM_DATA
            item.refresh()
            self.assertEqual(
                item.user_name,
                GET_MODEL_ITEM_DATA.get(ITEM).get('user_name').get(STRING_SHORT))

    def test_complex_key(self):
        """
        Model with complex key
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), COMPLEX_TABLE_DATA
            item = ComplexKeyModel('test')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(COMPLEX_ITEM_DATA), COMPLEX_ITEM_DATA
            item.refresh()

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
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            args = req.call_args[1]
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            item.delete(user_id='bar')
            params = {
                'key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'expected': {
                    'user_id': {
                        'Value': {'S': 'bar'},
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            args = req.call_args[1]
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            item.delete(user_id='bar')
            params = {
                'key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'expected': {
                    'user_id': {
                        'Value': {'S': 'bar'},
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            args = req.call_args[1]
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            item.delete(user_id='bar', email__contains='@', conditional_operator='AND')
            params = {
                'key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'expected': {
                    'email': {
                        'AttributeValueList': [
                            {'S': '@'}
                        ],
                        'ComparisonOperator': 'CONTAINS'
                    },
                    'user_id': {
                        'Value': {
                            'S': 'bar'
                        }
                    }
                },
                'conditional_operator': 'AND',
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            args = req.call_args[1]
            self.assertEqual(args, params)

    def test_update_item(self):
        """
        Model.update_item
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), SIMPLE_MODEL_TABLE_DATA
            item = SimpleUserModel('foo', email='bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {}
            item.save()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            item.update_item('views', 10, action='add')
            args = req.call_args[1]
            params = {
                'table_name': 'SimpleModel',
                'return_values': 'ALL_NEW',
                'key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'attribute_updates': {
                    'views': {
                        'Action': 'ADD',
                        'Value': {
                            'N': '10'
                        }
                    }
                },
                'return_consumed_capacity': 'TOTAL'
            }
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            item.update_item('views', 10, action='add', user_name='foo', email__not_contains='@')
            args = req.call_args[1]
            params = {
                'table_name': 'SimpleModel',
                'return_values': 'ALL_NEW',
                'key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'expected': {
                    'user_name': {
                        'Value': {'S': 'foo'}
                    },
                    'email': {
                        'AttributeValueList': [
                            {'S': '@'}
                        ],
                        'ComparisonOperator': 'NOT_CONTAINS'
                    },
                },
                'attribute_updates': {
                    'views': {
                        'Action': 'ADD',
                        'Value': {
                            'N': '10'
                        }
                    }
                },
                'return_consumed_capacity': 'TOTAL'
            }
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            item.update_item('views', 10, action='add', user_name__exists=False)
            args = req.call_args[1]
            params = {
                'table_name': 'SimpleModel',
                'return_values': 'ALL_NEW',
                'key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'expected': {
                    'user_name': {'Exists': False}
                },
                'attribute_updates': {
                    'views': {
                        'Action': 'ADD',
                        'Value': {
                            'N': '10'
                        }
                    }
                },
                'return_consumed_capacity': 'TOTAL'
            }
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            # Reproduces https://github.com/jlafon/PynamoDB/issues/34
            item.email = None
            item.update_item('views', 10, action='add')
            args = req.call_args[1]
            params = {
                'table_name': 'SimpleModel',
                'return_values': 'ALL_NEW',
                'key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'attribute_updates': {
                    'views': {
                        'Action': 'ADD',
                        'Value': {
                            'N': '10'
                        }
                    }
                },
                'return_consumed_capacity': 'TOTAL'
            }
            self.assertEqual(args, params)

    def test_save(self):
        """
        Model.save
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            item = UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
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
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }

            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
            item.save(email__exists=False)
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
                'expected': {
                    'email': {
                        'Exists': False
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
            item.save(email__exists=False, zip_code__null=False)
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
                'expected': {
                    'email': {
                        'Exists': False
                    },
                    'zip_code': {
                        'ComparisonOperator': 'NOT_NULL'
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
            item.save(user_name='bar', zip_code__null=True, email__contains='@', conditional_operator='OR')
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
                'conditional_operator': 'OR',
                'expected': {
                    'user_name': {
                        'Value': {'S': 'bar'}
                    },
                    'zip_code': {
                        'ComparisonOperator': 'NULL'
                    },
                    'email': {
                        'ComparisonOperator': 'CONTAINS',
                        'AttributeValueList': [
                            {'S': '@'}
                        ]
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
            item.save(custom_user_name='foo')
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
                'expected': {
                    'user_name': {
                        'Value': {'S': 'foo'}
                    }
                },
                'return_consumed_capacity': 'TOTAL',
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
                queried.append(item._serialize().get(RANGE))
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
            for item in UserModel.query('foo', user_id__gt='id-1', user_id__le='id-2'):
                queried.append(item._serialize())
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
                queried.append(item._serialize())
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
                queried.append(item._serialize())
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
                queried.append(item._serialize())
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
                queried.append(item._serialize())
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
                queried.append(item._serialize())
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
                queried.append(item._serialize())
            self.assertTrue(len(queried) == len(items))

        def fake_query(*args, **kwargs):
            start_key = kwargs.get(pythonic(EXCLUSIVE_START_KEY), None)
            if start_key:
                item_idx = 0
                for query_item in BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name):
                    item_idx += 1
                    if query_item == start_key:
                        break
                query_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[item_idx:item_idx+1]
            else:
                query_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[:1]
            data = {
                ITEMS: query_items,
                LAST_EVALUATED_KEY: query_items[-1] if len(query_items) else None
            }
            return HttpOK(data), data

        mock_query = MagicMock()
        mock_query.side_effect = fake_query

        with patch(PATCH_METHOD, new=mock_query) as req:
            for item in UserModel.query('foo'):
                self.assertIsNotNone(item)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), CUSTOM_ATTR_NAME_INDEX_TABLE_DATA
            CustomAttrNameModel._get_meta_data()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {ITEMS: [CUSTOM_ATTR_NAME_ITEM_DATA.get(ITEM)]}
            for item in CustomAttrNameModel.query('bar', overidden_user_name__eq='foo'):
                self.assertIsNotNone(item)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []
            for item in UserModel.query(
                    'foo',
                    user_id__begins_with='id',
                    email__contains='@',
                    picture__null=False,
                    zip_code__between=[2, 3]):
                queried.append(item._serialize())
            params = {
                'key_conditions': {
                    'user_id': {
                        'AttributeValueList': [
                            {'S': 'id'}
                        ],
                        'ComparisonOperator': 'BEGINS_WITH'
                    },
                    'user_name': {
                        'AttributeValueList': [
                            {'S': 'foo'}
                        ],
                        'ComparisonOperator': 'EQ'
                    }
                },
                'query_filter': {
                    'email': {
                        'AttributeValueList': [
                            {'S': '@'}
                        ],
                        'ComparisonOperator': 'CONTAINS'
                    },
                    'zip_code': {
                        'ComparisonOperator': 'BETWEEN',
                        'AttributeValueList': [
                            {'N': '2'},
                            {'N': '3'}
                        ]
                    },
                    'picture': {
                        'ComparisonOperator': 'NOT_NULL'
                    }
                },
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            self.assertEqual(params, req.call_args[1])
            self.assertTrue(len(queried) == len(items))

    def test_scan(self):
        """
        Model.scan
        """
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            scanned_items = []
            for item in UserModel.scan():
                scanned_items.append(item._serialize().get(RANGE))
            self.assertListEqual(
                [item.get('user_id').get(STRING_SHORT) for item in items],
                scanned_items
            )

        def fake_scan(*args, **kwargs):
            start_key = kwargs.get(pythonic(EXCLUSIVE_START_KEY), None)
            if start_key:
                item_idx = 0
                for scan_item in BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name):
                    item_idx += 1
                    if scan_item == start_key:
                        break
                scan_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[item_idx:item_idx+1]
            else:
                scan_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[:1]
            data = {
                ITEMS: scan_items,
                LAST_EVALUATED_KEY: scan_items[-1] if len(scan_items) else None
            }
            return HttpOK(data), data

        mock_scan = MagicMock()
        mock_scan.side_effect = fake_scan

        with patch(PATCH_METHOD, new=mock_scan) as req:
            for item in UserModel.scan():
                self.assertIsNotNone(item)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            for item in UserModel.scan(user_id__contains='tux', zip_code__null=False, email__null=True):
                self.assertIsNotNone(item)
            params = {
                'return_consumed_capacity': 'TOTAL',
                'scan_filter': {
                    'user_id': {
                        'AttributeValueList': [
                            {'S': 'tux'}
                        ],
                        'ComparisonOperator': 'CONTAINS'
                    },
                    'zip_code': {
                        'ComparisonOperator': 'NOT_NULL'
                    },
                    'email': {
                        'ComparisonOperator': 'NULL'
                    }
                },
                'table_name': 'UserModel'
            }
            self.assertEquals(params, req.call_args[1])

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            for item in UserModel.scan(
                    user_id__contains='tux',
                    zip_code__null=False,
                    conditional_operator='OR',
                    email__null=True):
                self.assertIsNotNone(item)
            params = {
                'return_consumed_capacity': 'TOTAL',
                'scan_filter': {
                    'user_id': {
                        'AttributeValueList': [
                            {'S': 'tux'}
                        ],
                        'ComparisonOperator': 'CONTAINS'
                    },
                    'zip_code': {
                        'ComparisonOperator': 'NOT_NULL'
                    },
                    'email': {
                        'ComparisonOperator': 'NULL'
                    },
                },
                'conditional_operator': 'OR',
                'table_name': 'UserModel'
            }
            self.assertEquals(params, req.call_args[1])

    def test_get(self):
        """
        Model.get
        """
        def fake_dynamodb(*args, **kwargs):
            if kwargs == {'table_name': UserModel.Meta.table_name}:
                return HttpOK(MODEL_TABLE_DATA), MODEL_TABLE_DATA
            elif kwargs == {
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel',
                'key': {'user_name': {'S': 'foo'},
                        'user_id': {'S': 'bar'}}, 'consistent_read': False}:
                return HttpOK(GET_MODEL_ITEM_DATA), GET_MODEL_ITEM_DATA
            return HttpOK(), MODEL_TABLE_DATA

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            item = UserModel.get(
                'foo',
                'bar'
            )
            self.assertEqual(item._get_keys(), {'user_id': 'bar', 'user_name': 'foo'})
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
                'return_consumed_capacity': 'TOTAL',
                'table_name': 'UserModel'
            }
            self.assertEqual(req.call_args[1], params)
            item.zip_code = 88030
            self.assertEqual(item.zip_code, 88030)

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
            self.assertRaises(UserModel.DoesNotExist, UserModel.get, 'foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), CUSTOM_ATTR_NAME_INDEX_TABLE_DATA
            CustomAttrNameModel._get_meta_data()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), {"ConsumedCapacity": {"CapacityUnits": 0.5, "TableName": "UserModel"}}
            self.assertRaises(CustomAttrNameModel.DoesNotExist, CustomAttrNameModel.get, 'foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
            self.assertRaises(CustomAttrNameModel.DoesNotExist, CustomAttrNameModel.get, 'foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), CUSTOM_ATTR_NAME_ITEM_DATA
            item = CustomAttrNameModel.get('foo', 'bar')
            self.assertEqual(item.overidden_attr, CUSTOM_ATTR_NAME_ITEM_DATA['Item']['foo_attr']['S'])
            self.assertEqual(item.overidden_user_name, CUSTOM_ATTR_NAME_ITEM_DATA['Item']['user_name']['S'])
            self.assertEqual(item.overidden_user_id, CUSTOM_ATTR_NAME_ITEM_DATA['Item']['user_id']['S'])

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
            params = {
                'return_consumed_capacity': 'TOTAL',
                'request_items': {
                    'SimpleModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-9'}},
                            {'user_name': {'S': 'hash-8'}},
                            {'user_name': {'S': 'hash-7'}},
                            {'user_name': {'S': 'hash-6'}},
                            {'user_name': {'S': 'hash-5'}},
                            {'user_name': {'S': 'hash-4'}},
                            {'user_name': {'S': 'hash-3'}},
                            {'user_name': {'S': 'hash-2'}},
                            {'user_name': {'S': 'hash-1'}},
                            {'user_name': {'S': 'hash-0'}}
                        ]
                    }
                }
            }
            self.assertEqual(params, req.call_args[1])

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), MODEL_TABLE_DATA
            UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            item_keys = [('hash-{0}'.format(x), '{0}'.format(x)) for x in range(10)]
            item_keys_copy = list(item_keys)
            req.return_value = HttpOK(), BATCH_GET_ITEMS
            for item in UserModel.batch_get(item_keys):
                self.assertIsNotNone(item)
            self.assertEqual(item_keys, item_keys_copy)
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

        def fake_batch_get(*batch_args, **kwargs):
            if pythonic(REQUEST_ITEMS) in kwargs:
                batch_item = kwargs.get(pythonic(REQUEST_ITEMS)).get(UserModel.Meta.table_name).get(KEYS)[0]
                batch_items = kwargs.get(pythonic(REQUEST_ITEMS)).get(UserModel.Meta.table_name).get(KEYS)[1:]
                response = {
                    UNPROCESSED_KEYS: {
                        UserModel.Meta.table_name: {
                            KEYS: batch_items
                        }
                    },
                    RESPONSES: {
                        UserModel.Meta.table_name: [batch_item]
                    }
                }
                return HttpOK(response), response
            return HttpOK({}), {}

        batch_get_mock = MagicMock()
        batch_get_mock.side_effect = fake_batch_get

        with patch(PATCH_METHOD, new=batch_get_mock) as req:
            item_keys = [('hash-{0}'.format(x), '{0}'.format(x)) for x in range(200)]
            for item in UserModel.batch_get(item_keys):
                self.assertIsNotNone(item)

    def test_batch_write(self):
        """
        Model.batch_write
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}

            with UserModel.batch_write(auto_commit=False) as batch:
                pass

            with UserModel.batch_write() as batch:
                self.assertIsNone(batch.commit())

            with self.assertRaises(ValueError):
                with UserModel.batch_write(auto_commit=False) as batch:
                    items = [UserModel('hash-{0}'.format(x), '{0}'.format(x)) for x in range(26)]
                    for item in items:
                        batch.delete(item)
                    self.assertRaises(ValueError, batch.save, UserModel('asdf', '1234'))

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

        def fake_unprocessed_keys(*args, **kwargs):
            if pythonic(REQUEST_ITEMS) in kwargs:
                batch_items = kwargs.get(pythonic(REQUEST_ITEMS)).get(UserModel.Meta.table_name)[1:]
                unprocessed = {
                    UNPROCESSED_KEYS: {
                        UserModel.Meta.table_name: batch_items
                    }
                }
                return HttpOK(unprocessed), unprocessed
            return HttpOK({}), {}

        batch_write_mock = MagicMock()
        batch_write_mock.side_effect = fake_unprocessed_keys

        with patch(PATCH_METHOD, new=batch_write_mock) as req:
            items = [UserModel('hash-{0}'.format(x), '{0}'.format(x)) for x in range(500)]
            for item in items:
                batch.save(item)

    def test_index_queries(self):
        """
        Models.Index.Query
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), CUSTOM_ATTR_NAME_INDEX_TABLE_DATA
            CustomAttrNameModel._get_meta_data()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), INDEX_TABLE_DATA
            IndexedModel._get_connection().describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), LOCAL_INDEX_TABLE_DATA
            LocalIndexedModel._get_meta_data()

        self.assertEqual(IndexedModel.include_index.Meta.index_name, "non_key_idx")

        queried = []
        with patch(PATCH_METHOD) as req:
            with self.assertRaises(ValueError):
                for item in IndexedModel.email_index.query('foo', user_id__between=['id-1', 'id-3']):
                    queried.append(item._serialize().get(RANGE))

        with patch(PATCH_METHOD) as req:
            with self.assertRaises(ValueError):
                for item in IndexedModel.email_index.query('foo', user_name__startswith='foo'):
                    queried.append(item._serialize().get(RANGE))

        with patch(PATCH_METHOD) as req:
            with self.assertRaises(ValueError):
                for item in IndexedModel.email_index.query('foo', name='foo'):
                    queried.append(item._serialize().get(RANGE))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                item['email'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []

            for item in IndexedModel.email_index.query('foo', limit=2, user_name__begins_with='bar'):
                queried.append(item._serialize())

            params = {
                'key_conditions': {
                    'user_name': {
                        'ComparisonOperator': 'BEGINS_WITH',
                        'AttributeValueList': [
                            {
                                'S': u'bar'
                            }
                        ]
                    },
                    'email': {
                        'ComparisonOperator': 'EQ',
                        'AttributeValueList': [
                            {
                                'S': u'foo'
                            }
                        ]
                    }
                },
                'index_name': 'custom_idx_name',
                'table_name': 'IndexedModel',
                'return_consumed_capacity': 'TOTAL',
                'limit': 2
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                item['email'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []

            for item in LocalIndexedModel.email_index.query(
                    'foo',
                    limit=1,
                    user_name__begins_with='bar',
                    aliases__contains=1):
                queried.append(item._serialize())

            params = {
                'key_conditions': {
                    'user_name': {
                        'ComparisonOperator': 'BEGINS_WITH',
                        'AttributeValueList': [
                            {
                                'S': u'bar'
                            }
                        ]
                    },
                    'email': {
                        'ComparisonOperator': 'EQ',
                        'AttributeValueList': [
                            {
                                'S': u'foo'
                            }
                        ]
                    }
                },
                'query_filter': {
                    'aliases': {
                        'AttributeValueList': [
                            {
                                'SS': ['1']
                            }
                        ],
                        'ComparisonOperator': 'CONTAINS'
                    }
                },
                'index_name': 'email_index',
                'table_name': 'LocalIndexedModel',
                'return_consumed_capacity': 'TOTAL',
                'limit': 1
            }
            self.assertEqual(req.call_args[1], params)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            queried = []

            for item in CustomAttrNameModel.uid_index.query('foo', limit=2, user_name__begins_with='bar'):
                queried.append(item._serialize())

            params = {
                'key_conditions': {
                    'user_name': {
                        'ComparisonOperator': 'BEGINS_WITH',
                        'AttributeValueList': [
                            {
                                'S': u'bar'
                            }
                        ]
                    },
                    'user_id': {
                        'ComparisonOperator': 'EQ',
                        'AttributeValueList': [
                            {
                                'S': u'foo'
                            }
                        ]
                    }
                },
                'index_name': 'uid_index',
                'table_name': 'CustomAttrModel',
                'return_consumed_capacity': 'TOTAL',
                'limit': 2
            }
            self.assertEqual(req.call_args[1], params)

    def test_multiple_indices_share_non_key_attribute(self):
        """
        Models.Model
        """
        scope_args = {'count': 0}

        def fake_dynamodb(obj, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                return HttpBadRequest(), {}
            else:
                return HttpOK({}), {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            IndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = {
                'attribute_definitions': [
                    {'AttributeName': 'email', 'AttributeType': 'S'},
                    {'AttributeName': 'numbers', 'AttributeType': 'NS'},
                    {'AttributeName': 'user_name', 'AttributeType': 'S'}
                ]
            }
            args = req.call_args[1]
            self.assert_dict_lists_equal(args['attribute_definitions'], params['attribute_definitions'])

        scope_args['count'] = 0

        with patch(PATCH_METHOD, new=fake_db) as req:
            GameModel.create_table()
            params = {
                'key_schema': [
                    {'KeyType': 'HASH', 'AttributeName': 'player_id'},
                    {'KeyType': 'RANGE', 'AttributeName': 'created_time'}
                ],
                'local_secondary_indexes': [
                    {
                        'KeySchema': [
                            {'KeyType': 'HASH', 'AttributeName': 'player_id'},
                            {'KeyType': 'RANGE', 'AttributeName': 'winner_id'}
                        ],
                        'IndexName': 'player_opponent_index',
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ],
                'table_name': 'GameModel',
                'provisioned_throughput': {'WriteCapacityUnits': 1, 'ReadCapacityUnits': 1},
                'global_secondary_indexes': [
                    {
                        'ProvisionedThroughput': {'WriteCapacityUnits': 1, 'ReadCapacityUnits': 1},
                        'KeySchema': [
                            {'KeyType': 'HASH', 'AttributeName': 'winner_id'},
                            {'KeyType': 'RANGE', 'AttributeName': 'created_time'}
                        ],
                        'IndexName': 'opponent_time_index',
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ],
                'attribute_definitions': [
                    {'AttributeName': 'created_time', 'AttributeType': 'S'},
                    {'AttributeName': 'player_id', 'AttributeType': 'S'},
                    {'AttributeName': 'winner_id', 'AttributeType': 'S'}
                ]
            }
            args = req.call_args[1]
            for key in ['key_schema', 'attribute_definitions', 'local_secondary_indexes', 'global_secondary_indexes']:
                self.assert_dict_lists_equal(args[key], params[key])

    def test_global_index(self):
        """
        Models.GlobalSecondaryIndex
        """
        self.assertIsNotNone(IndexedModel.email_index._hash_key_attribute())
        self.assertEqual(IndexedModel.email_index.Meta.projection.projection_type, AllProjection.projection_type)
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), INDEX_TABLE_DATA
            with self.assertRaises(ValueError):
                IndexedModel('foo', 'bar')
            IndexedModel._get_meta_data()

        scope_args = {'count': 0}

        def fake_dynamodb(obj, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                return HttpBadRequest(), {}
            else:
                return HttpOK({}), {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
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
            schema = IndexedModel.email_index._get_schema()
            args = req.call_args[1]
            self.assertEqual(
                args['global_secondary_indexes'][0]['ProvisionedThroughput'],
                {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 1
                }
            )
            self.assert_dict_lists_equal(schema['key_schema'], params['key_schema'])
            self.assert_dict_lists_equal(schema['attribute_definitions'], params['attribute_definitions'])

    def test_local_index(self):
        """
        Models.LocalSecondaryIndex
        """
        with self.assertRaises(ValueError):
            with patch(PATCH_METHOD) as req:
                req.return_value = HttpOK(), LOCAL_INDEX_TABLE_DATA
                # This table has no range key
                LocalIndexedModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), LOCAL_INDEX_TABLE_DATA
            LocalIndexedModel('foo')

        schema = IndexedModel._get_indexes()

        expected = {
            'local_secondary_indexes': [
                {
                    'key_schema': [
                        {'KeyType': 'HASH', 'AttributeName': 'email'},
                        {'KeyType': 'RANGE', 'AttributeName': 'numbers'}
                    ],
                    'index_name': 'include_index',
                    'projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': ['numbers']
                    }
                }
            ],
            'global_secondary_indexes': [
                {
                    'key_schema': [
                        {'KeyType': 'HASH', 'AttributeName': 'email'},
                        {'KeyType': 'RANGE', 'AttributeName': 'numbers'}
                    ],
                    'index_name': 'email_index',
                    'projection': {'ProjectionType': 'ALL'},
                    'provisioned_throughput': {
                        'WriteCapacityUnits': 1,
                        'ReadCapacityUnits': 2
                    }
                }
            ],
            'attribute_definitions': [
                {'attribute_type': 'S', 'attribute_name': 'email'},
                {'attribute_type': 'NS', 'attribute_name': 'numbers'},
                {'attribute_type': 'S', 'attribute_name': 'email'},
                {'attribute_type': 'NS', 'attribute_name': 'numbers'}
            ]
        }
        self.assert_dict_lists_equal(
            schema['attribute_definitions'],
            expected['attribute_definitions']
        )
        self.assertEqual(schema['local_secondary_indexes'][0]['projection']['ProjectionType'], 'INCLUDE')
        self.assertEqual(schema['local_secondary_indexes'][0]['projection']['NonKeyAttributes'], ['numbers'])

        scope_args = {'count': 0}

        def fake_dynamodb(obj, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                return HttpBadRequest(), {}
            else:
                return HttpOK(content={}), {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            LocalIndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = OrderedDict({
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
            })
            schema = LocalIndexedModel.email_index._get_schema()
            args = req.call_args[1]
            self.assert_dict_lists_equal(schema['attribute_definitions'], params['attribute_definitions'])
            self.assert_dict_lists_equal(schema['key_schema'], params['key_schema'])
            self.assertTrue('ProvisionedThroughput' not in args['local_secondary_indexes'][0])

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

        with self.assertRaises(ValueError):
            class BadIndex(Index):
                pass
            BadIndex()

        with self.assertRaises(ValueError):
            class BadIndex(Index):
                class Meta:
                    pass
                pass
            BadIndex()

    def test_throttle(self):
        """
        Throttle.add_record
        """
        throt = Throttle(30)
        throt.add_record(None)
        for i in range(10):
            throt.add_record(1)
            throt.throttle()
        for i in range(2):
            throt.add_record(50)
            throt.throttle()

    def test_old_style_model_exception(self):
        """
        Display warning for pre v1.0 Models
        """
        with self.assertRaises(AttributeError):
            OldStyleModel._get_meta_data()

        with self.assertRaises(AttributeError):
            OldStyleModel.exists()

    def test_dumps(self):
        """
        Model.dumps
        """
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                item['email'] = {STRING_SHORT: 'email-{0}'.format(random.randint(0, 65536))}
                item['picture'] = {BINARY_SHORT: BINARY_ATTR_DATA}
                items.append(item)
            req.return_value = HttpOK({'Items': items}), {'Items': items}
            content = UserModel.dumps()
            serialized_items = json.loads(content)
            for original, new_item in zip(items, serialized_items):
                self.assertEqual(new_item[0], original['user_name'][STRING_SHORT])
                self.assertEqual(new_item[1][pythonic(ATTRIBUTES)]['zip_code']['N'], original['zip_code']['N'])
                self.assertEqual(new_item[1][pythonic(ATTRIBUTES)]['email']['S'], original['email']['S'])
                self.assertEqual(new_item[1][pythonic(ATTRIBUTES)]['picture']['B'], original['picture']['B'])

    def test_loads(self):
        """
        Model.loads
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK({}), {}
            UserModel.loads(json.dumps(SERIALIZED_TABLE_DATA))

        args = {
            'UserModel': [
                {
                    'PutRequest': {
                        'Item': {
                            'user_id': {'S': u'id-0'},
                            'callable_field': {'N': '42'},
                            'user_name': {'S': u'foo'},
                            'email': {'S': u'email-7980'},
                            'picture': {
                                "B": "aGVsbG8sIHdvcmxk"
                            },
                            'zip_code': {'N': '88030'}
                        }
                    }
                },
                {
                    'PutRequest': {
                        'Item': {
                            'user_id': {'S': u'id-1'},
                            'callable_field': {'N': '42'},
                            'user_name': {'S': u'foo'},
                            'email': {'S': u'email-19770'},
                            'picture': {
                                "B": "aGVsbG8sIHdvcmxk"
                            },
                            'zip_code': {'N': '88030'}
                        }
                    }
                }
            ]
        }
        self.assert_dict_lists_equal(req.call_args[1]['request_items']['UserModel'], args['UserModel'])

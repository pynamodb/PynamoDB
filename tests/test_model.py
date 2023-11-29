"""
Test model API
"""
import base64
import json
import copy
import re
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest import TestCase

from botocore.client import ClientError
import pytest

from .deep_eq import deep_eq
from pynamodb.exceptions import DoesNotExist, TableError, PutError, AttributeDeserializationError
from pynamodb.constants import (
    ITEM, STRING, ALL, KEYS_ONLY, INCLUDE, REQUEST_ITEMS, UNPROCESSED_KEYS, CAMEL_COUNT,
    RESPONSES, KEYS, ITEMS, LAST_EVALUATED_KEY, EXCLUSIVE_START_KEY, ATTRIBUTES, BINARY,
    UNPROCESSED_ITEMS, DEFAULT_ENCODING, MAP, LIST, NUMBER, SCANNED_COUNT,
)
from pynamodb.models import Model
from pynamodb.indexes import (
    GlobalSecondaryIndex, LocalSecondaryIndex, AllProjection,
    IncludeProjection, KeysOnlyProjection, Index
)
from pynamodb.attributes import (
    DiscriminatorAttribute, UnicodeAttribute, NumberAttribute, BinaryAttribute, UTCDateTimeAttribute,
    UnicodeSetAttribute, NumberSetAttribute, BinarySetAttribute, MapAttribute,
    BooleanAttribute, ListAttribute, TTLAttribute, VersionAttribute)
from .data import (
    MODEL_TABLE_DATA, GET_MODEL_ITEM_DATA,
    BATCH_GET_ITEMS, SIMPLE_BATCH_GET_ITEMS,
    COMPLEX_ITEM_DATA, DOG_TABLE_DATA,
    CUSTOM_ATTR_NAME_ITEM_DATA,
    OFFICE_EMPLOYEE_MODEL_TABLE_DATA,
    GET_OFFICE_EMPLOYEE_ITEM_DATA, GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL,
    GROCERY_LIST_MODEL_TABLE_DATA, GET_GROCERY_LIST_ITEM_DATA,
    GET_OFFICE_ITEM_DATA, OFFICE_MODEL_TABLE_DATA, COMPLEX_MODEL_TABLE_DATA, COMPLEX_MODEL_ITEM_DATA,
    CAR_MODEL_TABLE_DATA, FULL_CAR_MODEL_ITEM_DATA, CAR_MODEL_WITH_NULL_ITEM_DATA,
    INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA,
    BOOLEAN_MODEL_TABLE_DATA, BOOLEAN_MODEL_FALSE_ITEM_DATA, BOOLEAN_MODEL_TRUE_ITEM_DATA,
    TREE_MODEL_TABLE_DATA, TREE_MODEL_ITEM_DATA,
    EXPLICIT_RAW_MAP_MODEL_TABLE_DATA, EXPLICIT_RAW_MAP_MODEL_ITEM_DATA,
    EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_ITEM_DATA, EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_TABLE_DATA,
)

from unittest.mock import patch, MagicMock

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'


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
    icons = BinarySetAttribute(legacy_encoding=False)


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
    icons = BinarySetAttribute(legacy_encoding=False)


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
    icons = BinarySetAttribute(legacy_encoding=False)
    views = NumberAttribute(null=True)
    is_active = BooleanAttribute(null=True)
    signature = UnicodeAttribute(null=True)
    ttl = TTLAttribute(null=True)


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
    picture = BinaryAttribute(null=True, legacy_encoding=False)
    zip_code = NumberAttribute(null=True)
    email = UnicodeAttribute(default='needs_email')
    callable_field = NumberAttribute(default=lambda: 42)
    ttl = TTLAttribute(null=True)


class BatchModel(Model):
    """
    A testing model
    """

    class Meta:
        table_name = 'BatchModel'
        max_retry_attempts = 0

    user_name = UnicodeAttribute(hash_key=True)



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


class BillingModeOnDemandModel(Model):
    """
    A testing model
    """

    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = 'BillingModeOnDemandModel'

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


class Location(MapAttribute):

    lat = NumberAttribute(attr_name='latitude')
    lng = NumberAttribute(attr_name='longitude')
    name = UnicodeAttribute()


class Person(MapAttribute):

    fname = UnicodeAttribute(attr_name='firstName')
    lname = UnicodeAttribute(null=True)
    age = NumberAttribute(null=True)
    is_male = BooleanAttribute(attr_name='is_dude')

    def foo(self):
        return 1


class ComplexModel(Model):
    class Meta:
        table_name = 'ComplexModel'
    person = Person(attr_name='weird_person')
    key = NumberAttribute(hash_key=True)


class OfficeEmployee(Model):
    class Meta:
        table_name = 'OfficeEmployeeModel'

    office_employee_id = NumberAttribute(hash_key=True)
    person = Person()
    office_location = Location()

    def foo(self):
        return 1


class CarLocation(MapAttribute):
    lat = NumberAttribute(null=False)
    lng = NumberAttribute(null=False)


class CarInfoMap(MapAttribute):
    make = UnicodeAttribute(null=False)
    model = UnicodeAttribute(null=True)
    location = CarLocation(null=True)


class CarModel(Model):
    class Meta:
        table_name = 'CarModel'
    car_id = NumberAttribute(hash_key=True, null=False)
    car_info = CarInfoMap(null=False)


class CarModelWithNull(Model):
    class Meta:
        table_name = 'CarModelWithNull'
    car_id = NumberAttribute(hash_key=True, null=False)
    car_color = UnicodeAttribute(null=True)
    car_info = CarInfoMap(null=True)


class OfficeEmployeeMap(MapAttribute):

    office_employee_id = NumberAttribute()
    person = Person()
    office_location = Location()

    def cool_function(self):
        return 1


class GroceryList(Model):
    class Meta:
        table_name = 'GroceryListModel'

    store_name = UnicodeAttribute(hash_key=True)
    groceries = ListAttribute()


class Office(Model):
    class Meta:
        table_name = 'OfficeModel'
    office_id = NumberAttribute(hash_key=True)
    address = Location()
    employees = ListAttribute(of=OfficeEmployeeMap)


class BooleanModel(Model):
    class Meta:
        table_name = 'BooleanTable'

    user_name = UnicodeAttribute(hash_key=True)
    is_human = BooleanAttribute()


class TreeLeaf(MapAttribute):
    value = NumberAttribute()


class TreeNode2(MapAttribute):
    value = NumberAttribute()
    left = TreeLeaf()
    right = TreeLeaf()


class TreeNode1(MapAttribute):
    value = NumberAttribute()
    left = TreeNode2()
    right = TreeNode2()


class TreeModel(Model):
    class Meta:
        table_name = 'TreeModelTable'

    tree_key = UnicodeAttribute(hash_key=True)
    left = TreeNode1()
    right = TreeNode1()


class ExplicitRawMapModel(Model):
    class Meta:
        table_name = 'ExplicitRawMapModel'
    map_id = NumberAttribute(hash_key=True, default=123)
    map_attr = MapAttribute()


class MapAttrSubClassWithRawMapAttr(MapAttribute):
    num_field = NumberAttribute()
    str_field = UnicodeAttribute()
    map_field = MapAttribute()


class ExplicitRawMapAsMemberOfSubClass(Model):
    class Meta:
        table_name = 'ExplicitRawMapAsMemberOfSubClass'
    map_id = NumberAttribute(hash_key=True)
    sub_attr = MapAttrSubClassWithRawMapAttr()


class Animal(Model):
    name = UnicodeAttribute(hash_key=True)


class Dog(Animal):
    class Meta:
        table_name = 'Dog'

    breed = UnicodeAttribute()


class TTLModel(Model):
    class Meta:
        table_name = 'TTLModel'
    user_name = UnicodeAttribute(hash_key=True)
    my_ttl = TTLAttribute(default_for_new=timedelta(minutes=1))


class VersionedModel(Model):
    class Meta:
        table_name = 'VersionedModel'

    name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    version = VersionAttribute()


class ModelTestCase(TestCase):
    """
    Tests for the models API
    """

    def assert_dict_lists_equal(self, list1, list2):
        """
        Compares two lists of dictionaries
        This function allows both the lists and dictionaries to have any order
        """
        if len(list1) != len(list2):
            raise AssertionError("Values not equal: {} {}".format(list1, list2))
        for d1_item in list1:
            found = False
            for d2_item in list2:
                if d2_item == d1_item:
                    found = True
            if not found:
                assert list1 == list2
                raise AssertionError("Values not equal: {} {}".format(list1, list2))

    def test_create_model(self):
        """
        Model.create_table
        """
        self.maxDiff = None
        scope_args = {'count': 0}

        def fake_dynamodb(*args):
            kwargs = args[1]
            if kwargs == {'TableName': UserModel.Meta.table_name}:
                if scope_args['count'] == 0:
                    return {}
                else:
                    return MODEL_TABLE_DATA
            else:
                return {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db):
            with patch("pynamodb.connection.TableConnection.describe_table") as req:
                req.return_value = None
                with self.assertRaises(TableError):
                    UserModel.create_table(read_capacity_units=2, write_capacity_units=2, wait=True)

        with patch(PATCH_METHOD, new=fake_db) as req:
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2)

        # Test for default region
        assert UserModel.Meta.region == None
        assert UserModel.Meta.connect_timeout_seconds, 15
        assert UserModel.Meta.read_timeout_seconds == 30
        assert UserModel.Meta.max_retry_attempts == 3
        assert UserModel.Meta.max_pool_connections == 10

        assert UserModel._connection.connection._connect_timeout_seconds == 15
        assert UserModel._connection.connection._read_timeout_seconds == 30
        assert UserModel._connection.connection._max_retry_attempts_exception == 3
        assert UserModel._connection.connection._max_pool_connections == 10

        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2)
            # The default region is determined by botocore
            self.assertEqual(UserModel._connection.connection.region, None)

        # A table with a specified region
        self.assertEqual(RegionSpecificModel.Meta.region, 'us-west-1')
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            RegionSpecificModel.create_table(read_capacity_units=2, write_capacity_units=2)
            self.assertEqual(RegionSpecificModel._connection.connection.region, 'us-west-1')

        # A table with a specified host
        self.assertEqual(HostSpecificModel.Meta.host, 'http://localhost')
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            HostSpecificModel.create_table(read_capacity_units=2, write_capacity_units=2)
            self.assertEqual(HostSpecificModel._connection.connection.host, 'http://localhost')

        # A table with a specified capacity
        self.assertEqual(UserModel.Meta.read_capacity_units, 25)
        self.assertEqual(UserModel.Meta.write_capacity_units, 25)

        # A table with billing_mode set as on_demand
        self.assertEqual(BillingModeOnDemandModel.Meta.billing_mode, 'PAY_PER_REQUEST')
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            BillingModeOnDemandModel.create_table(read_capacity_units=2, write_capacity_units=2)

        UserModel._connection = None

        def fake_wait(*obj, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            elif scope_args['count'] == 1 or scope_args['count'] == 2:
                data = copy.deepcopy(MODEL_TABLE_DATA)
                data['Table']['TableStatus'] = 'Creating'
                scope_args['count'] += 1
                return data
            else:
                return MODEL_TABLE_DATA

        mock_wait = MagicMock()
        mock_wait.side_effect = fake_wait

        scope_args = {'count': 0}
        with patch(PATCH_METHOD, new=mock_wait) as req:
            UserModel.create_table(wait=True)
            params = {
                'AttributeDefinitions': [
                    {
                        'AttributeName': 'user_name',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'user_id',
                        'AttributeType': 'S'
                    }
                ],
                'KeySchema': [
                    {
                        'AttributeName': 'user_name',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'user_id',
                        'KeyType': 'RANGE'
                    }
                ],
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 25, 'WriteCapacityUnits': 25
                },
                'TableName': 'UserModel',
            }
            actual = req.call_args_list[1][0][1]
            self.assertEqual(sorted(actual.keys()), sorted(params.keys()))
            self.assertEqual(actual['TableName'], params['TableName'])
            self.assertEqual(actual['ProvisionedThroughput'], params['ProvisionedThroughput'])
            self.assert_dict_lists_equal(sorted(actual['KeySchema'], key=lambda x: x['AttributeName']),
                                         sorted(params['KeySchema'], key=lambda x: x['AttributeName']))
            # These come in random order
            self.assert_dict_lists_equal(sorted(actual['AttributeDefinitions'], key=lambda x: x['AttributeName']),
                                         sorted(params['AttributeDefinitions'], key=lambda x: x['AttributeName']))

        def bad_server(*args):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                return {}
            elif scope_args['count'] == 1 or scope_args['count'] == 2:
                return {}

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
        item = UserModel('foo', 'bar')
        self.assertEqual(item.email, 'needs_email')
        self.assertEqual(item.callable_field, 42)
        self.assertEqual(
            repr(item), "UserModel(callable_field=42, email='needs_email', custom_user_name='foo', user_id='bar')"
        )

        item = SimpleUserModel('foo')
        self.assertEqual(repr(item), "SimpleUserModel(user_name='foo')")
        self.assertRaises(ValueError, item.save)

        self.assertRaises(ValueError, UserModel.from_raw_data, None)

        item = CustomAttrNameModel('foo', 'bar', overidden_attr='test')
        self.assertEqual(item.overidden_attr, 'test')
        self.assertTrue(not hasattr(item, 'foo_attr'))

    def test_overridden_defaults(self):
        """
        Custom attribute names
        """
        schema = CustomAttrNameModel._get_schema()
        self.assertListEqual(
            schema['key_schema'],
            [
                {
                    'KeyType': 'RANGE',
                    'AttributeName': 'user_id'
                },
                {
                    'KeyType': 'HASH',
                    'AttributeName': 'user_name'
                },
            ],
        )
        self.assertListEqual(
            schema['attribute_definitions'],
            [
                {
                    'AttributeType': 'S',
                    'AttributeName': 'user_id'
                },
                {
                    'AttributeType': 'S',
                    'AttributeName': 'user_name'
                },
            ]
        )

    def test_overridden_attr_name(self):
        user = UserModel(custom_user_name="bob")
        self.assertEqual(user.custom_user_name, "bob")
        self.assertRaises(AttributeError, getattr, user, "user_name")

        self.assertRaises(ValueError, UserModel, user_name="bob")

    def test_refresh(self):
        """
        Model.refresh
        """
        item = UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            self.assertRaises(item.DoesNotExist, item.refresh)

        with patch(PATCH_METHOD) as req:
            req.return_value = GET_MODEL_ITEM_DATA
            item.picture = b'to-be-removed'
            item.refresh()
            self.assertEqual(
                item.custom_user_name,
                GET_MODEL_ITEM_DATA.get(ITEM).get('user_name').get(STRING))
            self.assertIsNone(item.picture)

    def test_complex_key(self):
        """
        Model with complex key
        """
        item = ComplexKeyModel('test')

        with patch(PATCH_METHOD) as req:
            req.return_value = COMPLEX_ITEM_DATA
            item.refresh()

    def test_delete_doesnt_do_validation_on_null_attributes(self):
        """
        Model.delete
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            CarModel('foo').delete()

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            with CarModel.batch_write() as batch:
                car = CarModel('foo')
                batch.delete(car)

    @patch('time.time')
    def test_update(self, mock_time):
        """
        Model.update
        """
        mock_time.side_effect = [1559692800]  # 2019-06-05 00:00:00 UTC
        item = SimpleUserModel(user_name='foo', is_active=True, email='foo@example.com', signature='foo', views=100)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            item.save()

        self.assertRaises(TypeError, item.update, actions={'not': 'a list'})

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "user_name": {
                        "S": "foo",
                    },
                    "email": {
                        "S": "foo@example.com",
                    },
                    "is_active": {
                        "NULL": None,
                    },
                    "signature": {
                        "NULL": None,
                    },
                    "aliases": {
                        "SS": {"bob"},
                    }
                }
            }
            item.update(actions=[
                SimpleUserModel.email.set('foo@example.com'),
                SimpleUserModel.views.remove(),
                SimpleUserModel.is_active.set(None),
                SimpleUserModel.signature.set(None),
                SimpleUserModel.custom_aliases.set(['bob']),
                SimpleUserModel.numbers.delete(0, 1),
                SimpleUserModel.ttl.set(timedelta(seconds=60)),
            ])

            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'SET #0 = :0, #1 = :1, #2 = :2, #3 = :3, #4 = :4 REMOVE #5 DELETE #6 :5',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'is_active',
                    '#2': 'signature',
                    '#3': 'aliases',
                    '#4': 'ttl',
                    '#5': 'views',
                    '#6': 'numbers'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo@example.com',
                    },
                    ':1': {
                        'NULL': True
                    },
                    ':2': {
                        'NULL': True
                    },
                    ':3': {
                        'SS': ['bob']
                    },
                    ':4': {
                        'N': str(1559692800 + 60)
                    },
                    ':5': {
                        'NS': ['0', '1']
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

            assert item.views is None
            self.assertEqual({'bob'}, item.custom_aliases)

    def test_update_doesnt_do_validation_on_null_attributes(self):
        item = CarModel(12345)
        item.car_info = CarInfoMap(make='Foo', model='Bar')
        item.car_info.location = CarLocation()  # two levels deep we have invalid Nones

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "car_id": {
                        "N": "12345",
                    },
                }
            }
            item.update([CarModel.car_id.set(6789)])

    def test_save(self):
        """
        Model.save
        """
        item = UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            item.save()
            args = req.call_args[0][1]
            params = {
                'Item': {
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
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }

            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            item.save(UserModel.email.does_not_exist())
            args = req.call_args[0][1]
            params = {
                'Item': {
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
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'email'
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            item.save(UserModel.email.does_not_exist() & UserModel.zip_code.exists())
            args = req.call_args[0][1]
            params = {
                'Item': {
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
                'ConditionExpression': '(attribute_not_exists (#0) AND attribute_exists (#1))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'zip_code'
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            item.save(
                (UserModel.custom_user_name == 'bar') | UserModel.zip_code.does_not_exist() | UserModel.email.contains('@')
            )
            args = req.call_args[0][1]
            params = {
                'Item': {
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
                'ConditionExpression': '((#0 = :0 OR attribute_not_exists (#1)) OR contains (#2, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'user_name',
                    '#1': 'zip_code',
                    '#2': 'email'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'bar'
                    },
                    ':1': {
                        'S': '@'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            item.save(UserModel.custom_user_name == 'foo')
            args = req.call_args[0][1]
            params = {
                'Item': {
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
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

    def test_filter_count(self):
        """
        Model.count(**filters)
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {'Count': 10, 'ScannedCount': 20}
            res = UserModel.count('foo')
            self.assertEqual(res, 10)
            args = req.call_args[0][1]
            params = {
                'KeyConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    }
                },
                'TableName': 'UserModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Select': 'COUNT'
            }
            deep_eq(args, params, _assert=True)

    def test_count(self):
        """
        Model.count()
        """

        def fake_dynamodb(*args, **kwargs):
            return MODEL_TABLE_DATA

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            res = UserModel.count()
            self.assertEqual(res, 42)
            args = req.call_args[0][1]
            params = {'TableName': 'UserModel'}
            self.assertEqual(args, params)

    def test_count_no_hash_key(self):
        with pytest.raises(ValueError):
            UserModel.count(filter_condition=(UserModel.zip_code <= '94117'))

    def test_index_count(self):
        """
        Model.index.count()
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {'Count': 42, 'ScannedCount': 42}
            res = CustomAttrNameModel.uid_index.count(
                'foo',
                filter_condition=CustomAttrNameModel.overidden_user_name.startswith('bar'),
                limit=2)
            self.assertEqual(res, 42)
            args = req.call_args[0][1]
            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': 'begins_with (#1, :1)',
                'ExpressionAttributeNames': {
                    '#0': 'user_id',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    }
                },
                'Limit': 2,
                'IndexName': 'uid_index',
                'TableName': 'CustomAttrModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Select': 'COUNT'
            }
            deep_eq(args, params, _assert=True)

    def test_index_multipage_count(self):
        with patch(PATCH_METHOD) as req:
            last_evaluated_key = {
                'user_name': {'S': u'user'},
                'user_id': {'S': '1234'},
            }
            req.side_effect = [
                {'Count': 1000, 'ScannedCount': 1000, 'LastEvaluatedKey': last_evaluated_key},
                {'Count': 42, 'ScannedCount': 42}
            ]
            res = CustomAttrNameModel.uid_index.count('foo')
            self.assertEqual(res, 1042)

            args_one = req.call_args_list[0][0][1]
            params_one = {
                'KeyConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_id'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    }
                },
                'IndexName': 'uid_index',
                'TableName': 'CustomAttrModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Select': 'COUNT'
            }

            args_two = req.call_args_list[1][0][1]
            params_two = copy.deepcopy(params_one)
            params_two['ExclusiveStartKey'] = last_evaluated_key

            deep_eq(args_one, params_one, _assert=True)
            deep_eq(args_two, params_two, _assert=True)

    def test_query_limit_greater_than_available_items_single_page(self):
        UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(5):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)

            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            results = list(UserModel.query('foo', limit=25))
            self.assertEqual(len(results), 5)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 25)

    def test_query_limit_identical_to_available_items_single_page(self):
        UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(5):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)

            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            results = list(UserModel.query('foo', limit=5))
            self.assertEqual(len(results), 5)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 5)

    def test_query_limit_less_than_available_items_multiple_page(self):
        UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(30):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)

            req.side_effect = [
                {'Count': 10, 'ScannedCount': 20, 'Items': items[:10], 'LastEvaluatedKey': {'user_id': 'x'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[10:20], 'LastEvaluatedKey': {'user_id': 'y'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[20:30], 'LastEvaluatedKey': {'user_id': 'z'}},
            ]
            results_iter = UserModel.query('foo', limit=25)
            results = list(results_iter)
            self.assertEqual(len(results), 25)
            self.assertEqual(len(req.mock_calls), 3)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 25)
            self.assertEqual(req.mock_calls[1][1][1]['Limit'], 25)
            self.assertEqual(req.mock_calls[2][1][1]['Limit'], 25)
            self.assertEqual(results_iter.last_evaluated_key, {'user_id': items[24]['user_id']})
            self.assertEqual(results_iter.total_count, 30)
            self.assertEqual(results_iter.page_iter.total_scanned_count, 60)

    def test_query_limit_less_than_available_and_page_size(self):
        UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(30):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)

            req.side_effect = [
                {'Count': 10, 'ScannedCount': 20, 'Items': items[:10], 'LastEvaluatedKey': {'user_id': 'x'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[10:20], 'LastEvaluatedKey': {'user_id': 'y'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[20:30], 'LastEvaluatedKey': {'user_id': 'x'}},
            ]
            results_iter = UserModel.query('foo', limit=25, page_size=10)
            results = list(results_iter)
            self.assertEqual(len(results), 25)
            self.assertEqual(len(req.mock_calls), 3)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 10)
            self.assertEqual(req.mock_calls[1][1][1]['Limit'], 10)
            self.assertEqual(req.mock_calls[2][1][1]['Limit'], 10)
            self.assertEqual(results_iter.last_evaluated_key, {'user_id': items[24]['user_id']})
            self.assertEqual(results_iter.total_count, 30)
            self.assertEqual(results_iter.page_iter.total_scanned_count, 60)

    def test_query_limit_greater_than_available_items_multiple_page(self):
        UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(30):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)

            req.side_effect = [
                {'Count': 10, 'ScannedCount': 20, 'Items': items[:10], 'LastEvaluatedKey': {'user_id': 'x'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[10:20], 'LastEvaluatedKey': {'user_id': 'y'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[20:30]},
            ]
            results_iter = UserModel.query('foo', limit=50)
            results = list(results_iter)
            self.assertEqual(len(results), 30)
            self.assertEqual(len(req.mock_calls), 3)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 50)
            self.assertEqual(req.mock_calls[1][1][1]['Limit'], 50)
            self.assertEqual(req.mock_calls[2][1][1]['Limit'], 50)
            self.assertEqual(results_iter.last_evaluated_key, None)
            self.assertEqual(results_iter.total_count, 30)
            self.assertEqual(results_iter.page_iter.total_scanned_count, 60)

    def test_query_limit_greater_than_available_items_and_page_size(self):
        UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(30):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)

            req.side_effect = [
                {'Count': 10, 'ScannedCount': 20, 'Items': items[:10], 'LastEvaluatedKey': {'user_id': 'x'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[10:20], 'LastEvaluatedKey': {'user_id': 'y'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[20:30]},
            ]
            results_iter = UserModel.query('foo', limit=50, page_size=10)
            results = list(results_iter)
            self.assertEqual(len(results), 30)
            self.assertEqual(len(req.mock_calls), 3)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 10)
            self.assertEqual(req.mock_calls[1][1][1]['Limit'], 10)
            self.assertEqual(req.mock_calls[2][1][1]['Limit'], 10)
            self.assertEqual(results_iter.last_evaluated_key, None)
            self.assertEqual(results_iter.total_count, 30)
            self.assertEqual(results_iter.page_iter.total_scanned_count, 60)

    def test_query_with_exclusive_start_key(self):
        UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(30):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)

            req.side_effect = [
                {'Count': 10, 'ScannedCount': 10, 'Items': items[10:20], 'LastEvaluatedKey': {'user_id': items[19]['user_id']}},
            ]
            results_iter = UserModel.query('foo', limit=10, page_size=10, last_evaluated_key={'user_id': items[9]['user_id']})
            self.assertEqual(results_iter.last_evaluated_key, {'user_id': items[9]['user_id']})

            results = list(results_iter)
            self.assertEqual(len(results), 10)
            self.assertEqual(len(req.mock_calls), 1)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 10)
            self.assertEqual(results_iter.last_evaluated_key, {'user_id': items[19]['user_id']})
            self.assertEqual(results_iter.total_count, 10)
            self.assertEqual(results_iter.page_iter.total_scanned_count, 10)

    def test_query_with_failure(self):
        items = [
            {
                **GET_MODEL_ITEM_DATA[ITEM],
                'user_id': {
                    STRING: f'id-{idx}'
                },
            }
            for idx in range(30)
        ]

        with patch(PATCH_METHOD) as req:
            req.side_effect = [
                Exception('bleep-bloop'),
                {'Count': 10, 'ScannedCount': 10, 'Items': items[0:10], 'LastEvaluatedKey': {'user_id': items[10]['user_id']}},
            ]
            results_iter = UserModel.query('foo', limit=10, page_size=10)

            with pytest.raises(Exception, match='bleep-bloop'):
                next(results_iter)

            first_item = next(results_iter)
            assert first_item.user_id == 'id-0'

            second_item = next(results_iter)
            assert second_item.user_id == 'id-1'

    def test_query(self):
        """
        Model.query
        """
        UserModel('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []
            for item in UserModel.query('foo', UserModel.user_id.between('id-1', 'id-3')):
                hash_key, range_key = item._get_serialized_keys()
                queried.append(range_key)
            self.assertListEqual(
                [item.get('user_id').get(STRING) for item in items],
                queried
            )

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []
            for item in UserModel.query('foo', UserModel.user_id < 'id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []
            for item in UserModel.query('foo', UserModel.user_id >= 'id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []
            for item in UserModel.query('foo', UserModel.user_id <= 'id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []
            for item in UserModel.query('foo', UserModel.user_id == 'id-1'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []
            for item in UserModel.query('foo', UserModel.user_id.startswith('id')):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []
            for item in UserModel.query('foo'):
                queried.append(item.serialize())
            self.assertTrue(len(queried) == len(items))

        def fake_query(*args):
            kwargs = args[1]
            start_key = kwargs.get(EXCLUSIVE_START_KEY, None)
            if start_key:
                item_idx = 0
                for query_item in BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name):
                    item_idx += 1
                    if query_item == start_key:
                        break
                query_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[item_idx:item_idx + 1]
            else:
                query_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[:1]
            data = {
                CAMEL_COUNT: len(query_items),
                ITEMS: query_items,
                SCANNED_COUNT: 2 * len(query_items),
                LAST_EVALUATED_KEY: query_items[-1] if len(query_items) else None
            }
            return data

        mock_query = MagicMock()
        mock_query.side_effect = fake_query

        with patch(PATCH_METHOD, new=mock_query) as req:
            for item in UserModel.query('foo'):
                self.assertIsNotNone(item)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []
            for item in UserModel.query(
                    'foo',
                    UserModel.user_id.startswith('id'),
                    UserModel.email.contains('@') & UserModel.picture.exists() & UserModel.zip_code.between(2, 3)):
                queried.append(item.serialize())
            params = {
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'FilterExpression': '((contains (#2, :2) AND attribute_exists (#3)) AND #4 BETWEEN :3 AND :4)',
                'ExpressionAttributeNames': {
                    '#0': 'user_name',
                    '#1': 'user_id',
                    '#2': 'email',
                    '#3': 'picture',
                    '#4': 'zip_code'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'id'
                    },
                    ':2': {
                        'S': '@'
                    },
                    ':3': {
                        'N': '2'
                    },
                    ':4': {
                        'N': '3'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            self.assertEqual(params, req.call_args[0][1])
            self.assertTrue(len(queried) == len(items))

    def test_query_with_discriminator(self):
        class ParentModel(Model):
            class Meta:
                table_name = 'polymorphic_table'
            id = UnicodeAttribute(hash_key=True)
            cls = DiscriminatorAttribute()

        class ChildModel(ParentModel, discriminator='Child'):
            foo = UnicodeAttribute()

        # register a model that subclasses Child to ensure queries return model subclasses
        class GrandchildModel(ChildModel, discriminator='Grandchild'):
            bar = UnicodeAttribute()

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                "Table": {
                    "AttributeDefinitions": [
                        {
                            "AttributeName": "id",
                            "AttributeType": "S"
                        }
                    ],
                    "CreationDateTime": 1.363729002358E9,
                    "ItemCount": 0,
                    "KeySchema": [
                        {
                            "AttributeName": "id",
                            "KeyType": "HASH"
                        }
                    ],
                    "ProvisionedThroughput": {
                        "NumberOfDecreasesToday": 0,
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5
                    },
                    "TableName": "polymorphic_table",
                    "TableSizeBytes": 0,
                    "TableStatus": "ACTIVE"
                }
            }
            ChildModel('hi', foo='there').save()

        with patch(PATCH_METHOD) as req:
            req.return_value = {'Count': 0, 'ScannedCount': 0, 'Items': []}
            for item in ChildModel.query('foo'):
                pass
            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': '#1 IN (:1, :2)',
                'ExpressionAttributeNames': {
                    '#0': 'id',
                    '#1': 'cls'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'Child'
                    },
                    ':2': {
                        'S': u'Grandchild'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'polymorphic_table'
            }
            self.assertEqual(params, req.call_args[0][1])

    def test_scan_limit_with_page_size(self):
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(30):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)

            req.side_effect = [
                {'Count': 10, 'ScannedCount': 20, 'Items': items[:10], 'LastEvaluatedKey': {'user_id': 'x'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[10:20], 'LastEvaluatedKey': {'user_id': 'y'}},
                {'Count': 10, 'ScannedCount': 20, 'Items': items[20:30], 'LastEvaluatedKey': {'user_id': 'z'}},
            ]
            results_iter = UserModel.scan(limit=25, page_size=10)
            results = list(results_iter)
            self.assertEqual(len(results), 25)
            self.assertEqual(len(req.mock_calls), 3)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 10)
            self.assertEqual(req.mock_calls[1][1][1]['Limit'], 10)
            self.assertEqual(req.mock_calls[2][1][1]['Limit'], 10)
            self.assertEqual(results_iter.last_evaluated_key, {'user_id': items[24]['user_id']})
            self.assertEqual(results_iter.total_count, 30)
            self.assertEqual(results_iter.page_iter.total_scanned_count, 60)

    def test_scan_limit(self):
        """
        Model.scan(limit)
        """

        def fake_scan(*args):
            scan_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)
            data = {
                CAMEL_COUNT: len(scan_items),
                ITEMS: scan_items,
                SCANNED_COUNT: 2 * len(scan_items),
            }
            return data

        mock_scan = MagicMock()
        mock_scan.side_effect = fake_scan

        with patch(PATCH_METHOD, new=mock_scan) as req:
            count = 0
            for item in UserModel.scan(limit=4):
                count += 1
                self.assertIsNotNone(item)
            self.assertEqual(len(req.mock_calls), 1)
            self.assertEqual(req.mock_calls[0][1][1]['Limit'], 4)
            self.assertEqual(count, 4)

        with patch(PATCH_METHOD, new=mock_scan) as req:
            count = 0
            for item in UserModel.scan(limit=4, consistent_read=True):
                count += 1
                self.assertIsNotNone(item)
            self.assertEqual(len(req.mock_calls), 2)
            self.assertEqual(req.mock_calls[1][1][1]['Limit'], 4)
            self.assertEqual(req.mock_calls[1][1][1]['ConsistentRead'], True)
            self.assertEqual(count, 4)

    def test_scan(self):
        """
        Model.scan
        """
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            scanned_items = []
            for item in UserModel.scan():
                hash_key, range_key = item._get_serialized_keys()
                scanned_items.append(range_key)
            self.assertListEqual(
                [item.get('user_id').get(STRING) for item in items],
                scanned_items
            )

        def fake_scan(*args):
            kwargs = args[1]
            start_key = kwargs.get(EXCLUSIVE_START_KEY, None)
            if start_key:
                item_idx = 0
                for scan_item in BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name):
                    item_idx += 1
                    if scan_item == start_key:
                        break
                scan_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[item_idx:item_idx + 1]
            else:
                scan_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[:1]
            data = {
                CAMEL_COUNT: len(scan_items),
                ITEMS: scan_items,
                SCANNED_COUNT: 2 * len(scan_items),
                LAST_EVALUATED_KEY: scan_items[-1] if len(scan_items) else None
            }
            return data

        mock_scan = MagicMock()
        mock_scan.side_effect = fake_scan

        with patch(PATCH_METHOD, new=mock_scan) as req:
            for item in UserModel.scan():
                self.assertIsNotNone(item)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            for item in UserModel.scan(
                    attributes_to_get=['email']):
                self.assertIsNotNone(item)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'ProjectionExpression': '#0',
                'ExpressionAttributeNames': {
                    '#0': 'email'
                },
                'TableName': 'UserModel'
            }
            self.assertEqual(params, req.call_args[0][1])

    def test_get(self):
        """
        Model.get
        """

        def fake_dynamodb(*args):
            kwargs = args[1]
            if kwargs == {'TableName': UserModel.Meta.table_name}:
                return MODEL_TABLE_DATA
            elif kwargs == {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel',
                'Key': {
                    'user_name': {'S': 'foo'},
                    'user_id': {'S': 'bar'}
                },
                'ConsistentRead': False}:
                    return GET_MODEL_ITEM_DATA
            return MODEL_TABLE_DATA

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            item = UserModel.get(
                'foo',
                'bar'
            )
            self.assertEqual(item._get_keys(), {'user_id': 'bar', 'user_name': 'foo'})
            params = {
                'ConsistentRead': False,
                'Key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            self.assertEqual(req.call_args[0][1], params)
            item.zip_code = 88030
            self.assertEqual(item.zip_code, 88030)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            self.assertRaises(UserModel.DoesNotExist, UserModel.get, 'foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            try:
                UserModel.get('foo')
            except SimpleUserModel.DoesNotExist:
                self.fail('DoesNotExist exceptions must be distinct per-model')
            except UserModel.DoesNotExist:
                pass

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            try:
                UserModel.get('foo')
            except DoesNotExist:
                pass
            except UserModel.DoesNotExist:
                self.fail('UserModel.Exception must derive from pynamodb.Exceptions.DoesNotExist')

        with patch(PATCH_METHOD) as req:
            req.return_value = {"ConsumedCapacity": {"CapacityUnits": 0.5, "TableName": "UserModel"}}
            self.assertRaises(CustomAttrNameModel.DoesNotExist, CustomAttrNameModel.get, 'foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            self.assertRaises(CustomAttrNameModel.DoesNotExist, CustomAttrNameModel.get, 'foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = CUSTOM_ATTR_NAME_ITEM_DATA
            item = CustomAttrNameModel.get('foo', 'bar')
            self.assertEqual(item.overidden_attr, CUSTOM_ATTR_NAME_ITEM_DATA['Item']['foo_attr']['S'])
            self.assertEqual(item.overidden_user_name, CUSTOM_ATTR_NAME_ITEM_DATA['Item']['user_name']['S'])
            self.assertEqual(item.overidden_user_id, CUSTOM_ATTR_NAME_ITEM_DATA['Item']['user_id']['S'])

    def test_batch_get(self):
        """
        Model.batch_get
        """
        self.maxDiff = None

        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_BATCH_GET_ITEMS
            item_keys = ['hash-{}'.format(x) for x in range(10)]
            for item in SimpleUserModel.batch_get(item_keys):
                self.assertIsNotNone(item)
            req.call_args[0][1]['RequestItems']['SimpleModel']['Keys'].sort(key=json.dumps)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'SimpleModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-0'}},
                            {'user_name': {'S': 'hash-1'}},
                            {'user_name': {'S': 'hash-2'}},
                            {'user_name': {'S': 'hash-3'}},
                            {'user_name': {'S': 'hash-4'}},
                            {'user_name': {'S': 'hash-5'}},
                            {'user_name': {'S': 'hash-6'}},
                            {'user_name': {'S': 'hash-7'}},
                            {'user_name': {'S': 'hash-8'}},
                            {'user_name': {'S': 'hash-9'}},
                        ]
                    }
                }
            }
            self.assertEqual(params, req.call_args[0][1])

        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_BATCH_GET_ITEMS
            item_keys = ['hash-{}'.format(x) for x in range(10)]
            for item in SimpleUserModel.batch_get(item_keys, attributes_to_get=['numbers']):
                self.assertIsNotNone(item)
            req.call_args[0][1]['RequestItems']['SimpleModel']['Keys'].sort(key=json.dumps)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'SimpleModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-0'}},
                            {'user_name': {'S': 'hash-1'}},
                            {'user_name': {'S': 'hash-2'}},
                            {'user_name': {'S': 'hash-3'}},
                            {'user_name': {'S': 'hash-4'}},
                            {'user_name': {'S': 'hash-5'}},
                            {'user_name': {'S': 'hash-6'}},
                            {'user_name': {'S': 'hash-7'}},
                            {'user_name': {'S': 'hash-8'}},
                            {'user_name': {'S': 'hash-9'}}
                        ],
                        'ProjectionExpression': '#0',
                        'ExpressionAttributeNames': {
                            '#0': 'numbers'
                        }
                    }
                }
            }
            self.assertEqual(params, req.call_args[0][1])

        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_BATCH_GET_ITEMS
            item_keys = ['hash-{}'.format(x) for x in range(10)]
            for item in SimpleUserModel.batch_get(item_keys, consistent_read=True):
                self.assertIsNotNone(item)
            req.call_args[0][1]['RequestItems']['SimpleModel']['Keys'].sort(key=json.dumps)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'SimpleModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-0'}},
                            {'user_name': {'S': 'hash-1'}},
                            {'user_name': {'S': 'hash-2'}},
                            {'user_name': {'S': 'hash-3'}},
                            {'user_name': {'S': 'hash-4'}},
                            {'user_name': {'S': 'hash-5'}},
                            {'user_name': {'S': 'hash-6'}},
                            {'user_name': {'S': 'hash-7'}},
                            {'user_name': {'S': 'hash-8'}},
                            {'user_name': {'S': 'hash-9'}}
                        ],
                        'ConsistentRead': True
                    }
                }
            }
            self.assertEqual(params, req.call_args[0][1])

        with patch(PATCH_METHOD) as req:
            item_keys = [('hash-{}'.format(x), '{}'.format(x)) for x in range(10)]
            item_keys_copy = list(item_keys)
            req.return_value = BATCH_GET_ITEMS
            for item in UserModel.batch_get(item_keys):
                self.assertIsNotNone(item)
            self.assertEqual(item_keys, item_keys_copy)
            params = {
                'RequestItems': {
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
            args = req.call_args[0][1]
            self.assertTrue('RequestItems' in params)
            self.assertTrue('UserModel' in params['RequestItems'])
            self.assertTrue('Keys' in params['RequestItems']['UserModel'])
            self.assert_dict_lists_equal(
                params['RequestItems']['UserModel']['Keys'],
                args['RequestItems']['UserModel']['Keys'],
            )

        def fake_batch_get(*batch_args):
            kwargs = batch_args[1]
            if REQUEST_ITEMS in kwargs:
                batch_item = kwargs.get(REQUEST_ITEMS).get(UserModel.Meta.table_name).get(KEYS)[0]
                batch_items = kwargs.get(REQUEST_ITEMS).get(UserModel.Meta.table_name).get(KEYS)[1:]
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
                return response
            return {}

        batch_get_mock = MagicMock()
        batch_get_mock.side_effect = fake_batch_get

        with patch(PATCH_METHOD, new=batch_get_mock) as req:
            item_keys = [('hash-{}'.format(x), '{}'.format(x)) for x in range(200)]
            for item in UserModel.batch_get(item_keys):
                self.assertIsNotNone(item)

    def test_batch_get__range_key(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = {
                'UnprocessedKeys': {},
                'Responses': {
                    'UserModel': [],
                }
            }
            items = [(f'hash-{x}', f'range-{x}') for x in range(10)]
            _ = list(UserModel.batch_get(items))

            actual_keys = req.call_args[0][1]['RequestItems']['UserModel']['Keys']
            actual_keys.sort(key=json.dumps)
            assert actual_keys == [
                {'user_name': {'S': f'hash-{x}'}, 'user_id': {'S': f'range-{x}'}}
                for x in range(10)
            ]

    def test_batch_get__range_key__invalid__string(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = {
                'UnprocessedKeys': {},
                'Responses': {
                    'UserModel': [],
                }
            }
            with pytest.raises(
                ValueError,
                match=re.escape(
                    "Invalid key value 'ab': expected non-str iterable with exactly 2 elements (hash key, range key)"
                )
            ):
                _ = list(UserModel.batch_get(['ab']))

    def test_batch_get__range_key__invalid__3_elements(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = {
                'UnprocessedKeys': {},
                'Responses': {
                    'UserModel': [],
                }
            }
            with pytest.raises(
                ValueError,
                match=re.escape(
                    "Invalid key value ('a', 'b', 'c'): expected iterable with exactly 2 elements (hash key, range key)"
                )
            ):
                _ = list(UserModel.batch_get([('a', 'b', 'c')]))

    def test_batch_write(self):
        """
        Model.batch_write
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {}

            with UserModel.batch_write(auto_commit=False) as batch:
                pass

            with UserModel.batch_write() as batch:
                self.assertIsNone(batch.commit())

            with self.assertRaises(ValueError):
                with UserModel.batch_write(auto_commit=False) as batch:
                    items = [UserModel('hash-{}'.format(x), '{}'.format(x)) for x in range(26)]
                    for item in items:
                        batch.delete(item)
                    self.assertRaises(ValueError, batch.save, UserModel('asdf', '1234'))

            with UserModel.batch_write(auto_commit=False) as batch:
                items = [UserModel('hash-{}'.format(x), '{}'.format(x)) for x in range(25)]
                for item in items:
                    batch.delete(item)
                self.assertRaises(ValueError, batch.save, UserModel('asdf', '1234'))

            with UserModel.batch_write(auto_commit=False) as batch:
                items = [UserModel('hash-{}'.format(x), '{}'.format(x)) for x in range(25)]
                for item in items:
                    batch.save(item)
                self.assertRaises(ValueError, batch.save, UserModel('asdf', '1234'))

            with UserModel.batch_write() as batch:
                items = [UserModel('hash-{}'.format(x), '{}'.format(x)) for x in range(30)]
                for item in items:
                    batch.delete(item)

            with UserModel.batch_write() as batch:
                items = [UserModel('hash-{}'.format(x), '{}'.format(x)) for x in range(30)]
                for item in items:
                    batch.save(item)


    def test_batch_write_with_unprocessed(self):
        picture_blob = b'FFD8FFD8'

        items = []
        for idx in range(10):
            items.append(UserModel(
                'daniel',
                '{}'.format(idx),
                picture=picture_blob,
            ))

        unprocessed_items = []
        for idx in range(5, 10):
            unprocessed_items.append({
                'PutRequest': {
                    'Item': {
                        'custom_username': {STRING: 'daniel'},
                        'user_id': {STRING: '{}'.format(idx)},
                        'picture': {BINARY: base64.b64encode(picture_blob).decode(DEFAULT_ENCODING)}
                    }
                }
            })

        with patch(PATCH_METHOD) as req:
            req.side_effect = [
                {
                    UNPROCESSED_ITEMS: {
                        UserModel.Meta.table_name: unprocessed_items[:2],
                    },
                },
                {},
            ]

            with UserModel.batch_write() as batch:
                for item in items:
                    batch.save(item)

            self.assertEqual(len(req.mock_calls), 2)

    def test_batch_write_raises_put_error(self):
        items = []
        for idx in range(10):
            items.append(BatchModel(
                '{}'.format(idx)
            ))

        unprocessed_items = []
        for idx in range(5, 10):
            unprocessed_items.append({
                'PutRequest': {
                    'Item': {
                        'user_name': {STRING: 'daniel'},
                    }
                }
            })

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                UNPROCESSED_ITEMS: {
                    BatchModel.Meta.table_name: unprocessed_items[2:],
                }
            }
            with self.assertRaises(PutError):
                with BatchModel.batch_write() as batch:
                    for item in items:
                        batch.save(item)
            self.assertEqual(len(batch.failed_operations), 3)

    def test_index_queries(self):
        """
        Model.Index.Query
        """
        self.assertEqual(IndexedModel.include_index.Meta.index_name, "non_key_idx")

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING: 'id-{}'.format(idx)}
                item['email'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []

            for item in IndexedModel.email_index.query('foo', filter_condition=IndexedModel.user_name.startswith('bar'), limit=2):
                queried.append(item.serialize())

            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': 'begins_with (#1, :1)',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    }
                },
                'IndexName': 'custom_idx_name',
                'TableName': 'IndexedModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 2
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING: 'id-{}'.format(idx)}
                item['email'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []

            for item in LocalIndexedModel.email_index.query(
                    'foo',
                    filter_condition=LocalIndexedModel.user_name.startswith('bar') & LocalIndexedModel.aliases.contains('baz'),
                    limit=1):
                queried.append(item.serialize())

            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': '(begins_with (#1, :1) AND contains (#2, :2))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_name',
                    '#2': 'aliases'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    },
                    ':2': {
                        'S': u'baz'
                    }
                },
                'IndexName': 'email_index',
                'TableName': 'LocalIndexedModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 1
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING: 'id-{}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            queried = []

            for item in CustomAttrNameModel.uid_index.query(
                    'foo',
                    filter_condition=CustomAttrNameModel.overidden_user_name.startswith('bar'),
                    limit=2):
                queried.append(item.serialize())

            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': 'begins_with (#1, :1)',
                'ExpressionAttributeNames': {
                    '#0': 'user_id',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    }
                },
                'IndexName': 'uid_index',
                'TableName': 'CustomAttrModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 2
            }
            self.assertEqual(req.call_args[0][1], params)

    def test_multiple_indices_share_non_key_attribute(self):
        """
        Models.Model
        """
        scope_args = {'count': 0}

        def fake_dynamodb(*args, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            return {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            IndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = {
                'AttributeDefinitions': [
                    {'AttributeName': 'email', 'AttributeType': 'S'},
                    {'AttributeName': 'numbers', 'AttributeType': 'NS'},
                    {'AttributeName': 'user_name', 'AttributeType': 'S'}
                ]
            }
            args = req.call_args[0][1]
            self.assert_dict_lists_equal(args['AttributeDefinitions'], params['AttributeDefinitions'])

        scope_args['count'] = 0

        with patch(PATCH_METHOD, new=fake_db) as req:
            GameModel.create_table()
            params = {
                'KeySchema': [
                    {'KeyType': 'HASH', 'AttributeName': 'player_id'},
                    {'KeyType': 'RANGE', 'AttributeName': 'created_time'}
                ],
                'LocalSecondaryIndexes': [
                    {
                        'KeySchema': [
                            {'KeyType': 'HASH', 'AttributeName': 'player_id'},
                            {'KeyType': 'RANGE', 'AttributeName': 'winner_id'}
                        ],
                        'IndexName': 'player_opponent_index',
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ],
                'TableName': 'GameModel',
                'ProvisionedThroughput': {'WriteCapacityUnits': 1, 'ReadCapacityUnits': 1},
                'GlobalSecondaryIndexes': [
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
                'AttributeDefinitions': [
                    {'AttributeName': 'created_time', 'AttributeType': 'S'},
                    {'AttributeName': 'player_id', 'AttributeType': 'S'},
                    {'AttributeName': 'winner_id', 'AttributeType': 'S'}
                ]
            }
            args = req.call_args[0][1]
            for key in ['KeySchema', 'AttributeDefinitions', 'LocalSecondaryIndexes', 'GlobalSecondaryIndexes']:
                self.assert_dict_lists_equal(args[key], params[key])

    def test_global_index(self):
        """
        Models.GlobalSecondaryIndex
        """
        self.assertIsNotNone(IndexedModel.email_index._hash_key_attribute())
        self.assertEqual(IndexedModel.email_index.Meta.projection.projection_type, AllProjection.projection_type)

        scope_args = {'count': 0}

        def fake_dynamodb(*args, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            else:
                return {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            IndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            args = req.call_args[0][1]
            self.assert_dict_lists_equal(
                args['AttributeDefinitions'],
                [
                    {'AttributeName': 'user_name', 'AttributeType': 'S'},
                    {'AttributeName': 'email', 'AttributeType': 'S'},
                    {'AttributeName': 'numbers', 'AttributeType': 'NS'}
                ]
            )
            self.assert_dict_lists_equal(
                args['GlobalSecondaryIndexes'][0]['KeySchema'],
                [
                    {'AttributeName': 'email', 'KeyType': 'HASH'},
                    {'AttributeName': 'numbers', 'KeyType': 'RANGE'}
                ]
            )
            self.assertEqual(
                args['GlobalSecondaryIndexes'][0]['ProvisionedThroughput'],
                {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 1
                }
            )

    def test_local_index(self):
        """
        Models.LocalSecondaryIndex
        """
        schema = IndexedModel._get_schema()

        self.assertListEqual(
            schema['attribute_definitions'],
            [
                {
                    'AttributeType': 'S',
                    'AttributeName': 'user_name'
                },
                {
                    'AttributeType': 'NS',
                    'AttributeName': 'numbers'
                },
                {
                    'AttributeType': 'S',
                    'AttributeName': 'email'
                },
            ]
        )
        self.assertEqual(schema['local_secondary_indexes'][0]['projection']['ProjectionType'], 'INCLUDE')
        self.assertEqual(schema['local_secondary_indexes'][0]['projection']['NonKeyAttributes'], ['numbers'])

        scope_args = {'count': 0}

        def fake_dynamodb(*args, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            else:
                return {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            LocalIndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            schema = LocalIndexedModel.email_index._get_schema()
            args = req.call_args[0][1]
            self.assert_dict_lists_equal(
                schema['key_schema'],
                [
                    {
                        'AttributeName': 'email', 'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'numbers', 'KeyType': 'RANGE'
                    }
                ],
            )
            self.assertTrue('ProvisionedThroughput' not in args['LocalSecondaryIndexes'][0])

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

    def test_old_style_model_exception(self):
        """
        Display warning for pre v1.0 Models
        """
        with self.assertRaises(AttributeError):
            OldStyleModel.exists()

    def test_no_table_name_exception(self):
        """
        Display warning for Models without table names
        """
        class MissingTableNameModel(Model):
            class Meta:
                pass
            user_name = UnicodeAttribute(hash_key=True)
        with self.assertRaises(AttributeError):
            MissingTableNameModel.exists()

    def _get_office_employee(self):
        justin = Person(
            fname='Justin',
            lname='Phillips',
            age=31,
            is_male=True
        )
        loc = Location(
            lat=37.77461,
            lng=-122.3957216,
            name='Lyft HQ'
        )
        return OfficeEmployee(
            hash_key=None,
            range_key=None,
            office_employee_id=123,
            person=justin,
            office_location=loc
        )

    def _get_grocery_list(self):
        return GroceryList(store_name='Haight Street Market',
                           groceries=['bread', 1, 'butter', 6, 'milk', 1])

    def _get_complex_thing(self):
        justin = Person(
            fname='Justin',
            lname='Phillips',
            age=31,
            is_male=True
        )
        return ComplexModel(person=justin, key=123)

    def _get_office(self):
        justin = Person(
            fname='Justin',
            lname='Phillips',
            age=31,
            is_male=True
        )
        lei = Person(
            fname='Lei',
            lname='Ding',
            age=32,
            is_male=True
        )
        garrett = Person(
            fname='Garrett',
            lname='Heel',
            age=30,
            is_male=True
        )
        tanya = Person(
            fname='Tanya',
            lname='Ashkenazi',
            age=30,
            is_male=False
        )
        loc = Location(
            lat=37.77461,
            lng=-122.3957216,
            name='Lyft HQ'
        )
        emp1 = OfficeEmployeeMap(
            office_employee_id=123,
            person=justin,
            office_location=loc
        )
        emp2 = OfficeEmployeeMap(
            office_employee_id=124,
            person=lei,
            office_location=loc
        )
        emp3 = OfficeEmployeeMap(
            office_employee_id=125,
            person=garrett,
            office_location=loc
        )
        emp4 = OfficeEmployeeMap(
            office_employee_id=126,
            person=tanya,
            office_location=loc
        )
        return Office(
            office_id=3,
            address=loc,
            employees=[emp1, emp2, emp3, emp4]
        )

    def test_model_with_maps(self):
        office_employee = self._get_office_employee()
        with patch(PATCH_METHOD):
            office_employee.save()

    def test_model_with_list(self):
        grocery_list = self._get_grocery_list()
        with patch(PATCH_METHOD):
            grocery_list.save()

    def test_model_with_list_of_map(self):
        item = self._get_office()
        with patch(PATCH_METHOD):
            item.save()

    def test_model_with_nulls_validates(self):
        car_info = CarInfoMap(make='Dodge')
        item = CarModel(car_id=123, car_info=car_info)
        with patch(PATCH_METHOD):
            item.save()

    def test_model_with_invalid_data_does_not_validate__list_attr(self):
        office = Office(office_id=3, address=Location(lat=37.77461, lng=-122.3957216, name='Lyft HQ'))
        employee = OfficeEmployeeMap(
            office_employee_id=123,
            person=Person(is_male=False),
            office_location=Location(lat=37.77461, lng=-122.3957216, name='Lyft HQ'),
        )
        office.employees = [employee]
        employee.fname = None
        with patch(PATCH_METHOD):
            with self.assertRaises(ValueError) as cm:
                office.save()
            assert str(cm.exception) == "Attribute 'employees.[0].person.fname' cannot be None"

    def test_model_works_like_model(self):
        office_employee = self._get_office_employee()
        self.assertTrue(office_employee.person)
        self.assertEqual('Justin', office_employee.person.fname)
        self.assertEqual('Phillips', office_employee.person.lname)
        self.assertEqual(31, office_employee.person.age)
        self.assertEqual(True, office_employee.person.is_male)

    def test_list_works_like_list(self):
        grocery_list = self._get_grocery_list()
        self.assertTrue(grocery_list.groceries)
        self.assertEqual('butter', grocery_list.groceries[2])

    def test_complex_model_is_complex(self):
        complex_thing = self._get_complex_thing()
        self.assertTrue(complex_thing.person)
        self.assertEqual(complex_thing.person.fname, 'Justin')
        self.assertEqual(complex_thing.key, 123)

    def test_list_of_map_works_like_list_of_map(self):
        office = self._get_office()
        self.assertTrue(office.employees[1].person.is_male)
        self.assertFalse(office.employees[3].person.is_male)
        self.assertEqual(office.employees[2].person.fname, 'Garrett')
        self.assertEqual(office.employees[0].person.lname, 'Phillips')

    def test_invalid_map_model_raises(self):
        fake_db = self.database_mocker(CarModel, CAR_MODEL_TABLE_DATA,
                                       FULL_CAR_MODEL_ITEM_DATA, 'car_id', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            with self.assertRaises(ValueError) as cm:
                CarModel(car_id=2).save()
            assert str(cm.exception) == "Attribute 'car_info' cannot be None"

    def test_model_with_maps_retrieve_from_db(self):
        fake_db = self.database_mocker(OfficeEmployee, OFFICE_EMPLOYEE_MODEL_TABLE_DATA,
                                       GET_OFFICE_EMPLOYEE_ITEM_DATA, 'office_employee_id', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_OFFICE_EMPLOYEE_ITEM_DATA
            item = OfficeEmployee.get(123)
            self.assertEqual(
                item.person.fname,
                GET_OFFICE_EMPLOYEE_ITEM_DATA.get(ITEM).get('person').get(
                    MAP).get('firstName').get(STRING))

    def test_model_with_maps_with_nulls_retrieve_from_db(self):
        fake_db = self.database_mocker(OfficeEmployee, OFFICE_EMPLOYEE_MODEL_TABLE_DATA,
                                       GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL, 'office_employee_id', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL
            item = OfficeEmployee.get(123)
            self.assertEqual(
                item.person.fname,
                GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL.get(ITEM).get('person').get(
                    MAP).get('firstName').get(STRING))
            self.assertIsNone(item.person.age)
            self.assertIsNone(item.person.is_male)

    def test_model_with_maps_with_snake_case_attributes(self):
        fake_db = self.database_mocker(
            OfficeEmployee,
            OFFICE_EMPLOYEE_MODEL_TABLE_DATA,
            GET_OFFICE_EMPLOYEE_ITEM_DATA,
            'office_employee_id',
            'N',
            '123'
        )

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_OFFICE_EMPLOYEE_ITEM_DATA
            item = OfficeEmployee.get(123)
            self.assertEqual(
                item.person.fname,
                GET_OFFICE_EMPLOYEE_ITEM_DATA
                    .get(ITEM)
                    .get('person')
                    .get(MAP)
                    .get('firstName')
                    .get(STRING)
            )
        assert item.person.is_male
        with pytest.raises(AttributeError):
            _ = item.person.is_dude

    def test_model_with_list_retrieve_from_db(self):
        fake_db = self.database_mocker(GroceryList, GROCERY_LIST_MODEL_TABLE_DATA,
                                       GET_GROCERY_LIST_ITEM_DATA, 'store_name', 'S',
                                 'Haight Street Market')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_GROCERY_LIST_ITEM_DATA
            item = GroceryList.get('Haight Street Market')
            self.assertEqual(item.store_name, GET_GROCERY_LIST_ITEM_DATA.get(ITEM).get('store_name').get(STRING))
            self.assertEqual(
                item.groceries[2],
                GET_GROCERY_LIST_ITEM_DATA.get(ITEM).get('groceries').get(
                    LIST)[2].get(STRING))
            self.assertEqual(item.store_name, 'Haight Street Market')

    def test_model_with_list_of_map_retrieve_from_db(self):
        fake_db = self.database_mocker(Office, OFFICE_MODEL_TABLE_DATA,
                                       GET_OFFICE_ITEM_DATA, 'office_id', 'N',
                                 '6161')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_OFFICE_ITEM_DATA
            item = Office.get(6161)
            self.assertEqual(item.office_id,
                              int(GET_OFFICE_ITEM_DATA.get(ITEM).get('office_id').get(NUMBER)))
            self.assertEqual(item.office_id, 6161)
            self.assertEqual(
                item.employees[2].person.fname,
                GET_OFFICE_ITEM_DATA.get(ITEM).get('employees').get(
                    LIST)[2].get(MAP).get('person').get(MAP).get('firstName').get(STRING))

    def test_complex_model_retrieve_from_db(self):
        fake_db = self.database_mocker(ComplexModel, COMPLEX_MODEL_TABLE_DATA,
                                       COMPLEX_MODEL_ITEM_DATA, 'key', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = COMPLEX_MODEL_ITEM_DATA
            item = ComplexModel.get(123)
            self.assertEqual(item.key,
                              int(COMPLEX_MODEL_ITEM_DATA.get(ITEM).get(
                                  'key').get(NUMBER)))
            self.assertEqual(item.key, 123)
            self.assertEqual(
                item.person.fname,
                COMPLEX_MODEL_ITEM_DATA.get(ITEM).get('weird_person').get(
                    MAP).get('firstName').get(STRING))
            self.assertEqual(
                repr(item),
                "ComplexModel(key=123, person=Person(age=31, fname='Justin', is_male=True, lname='Phillips'))"
            )

    def database_mocker(self, model, table_data, item_data,
                        primary_key_name, primary_key_dynamo_type, primary_key_id):
        def fake_dynamodb(*args):
            kwargs = args[1]
            if kwargs == {'TableName': model.Meta.table_name}:
                return table_data
            elif kwargs == {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': model.Meta.table_name,
                'Key': {
                    primary_key_name: {primary_key_dynamo_type: primary_key_id},
                },
                'ConsistentRead': False}:
                return item_data
            return table_data
        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb
        return fake_db

    def test_car_model_retrieve_from_db(self):
        fake_db = self.database_mocker(CarModel, CAR_MODEL_TABLE_DATA,
                                       FULL_CAR_MODEL_ITEM_DATA, 'car_id', 'N', '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = FULL_CAR_MODEL_ITEM_DATA
            item = CarModel.get(123)
            self.assertEqual(item.car_id,
                              int(FULL_CAR_MODEL_ITEM_DATA.get(ITEM).get(
                                  'car_id').get(NUMBER)))
            self.assertEqual(item.car_info.make, 'Volkswagen')
            self.assertEqual(item.car_info.model, 'Beetle')

    def test_car_model_with_null_retrieve_from_db(self):
        fake_db = self.database_mocker(CarModel, CAR_MODEL_TABLE_DATA,
                                       CAR_MODEL_WITH_NULL_ITEM_DATA, 'car_id', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = CAR_MODEL_WITH_NULL_ITEM_DATA
            item = CarModel.get(123)
            self.assertEqual(item.car_id,
                              int(CAR_MODEL_WITH_NULL_ITEM_DATA.get(ITEM).get(
                                  'car_id').get(NUMBER)))
            self.assertEqual(item.car_info.make, 'Dodge')
            self.assertIsNone(item.car_info.model)

    def test_invalid_car_model_with_null_retrieve_from_db(self):
        fake_db = self.database_mocker(CarModel, CAR_MODEL_TABLE_DATA,
                                       INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA, 'car_id', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA
            item = CarModel.get(123)
            self.assertEqual(item.car_id,
                              int(INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA.get(ITEM).get(
                                  'car_id').get(NUMBER)))
            self.assertIsNone(item.car_info.make)

    def test_deserializing_bool_false_works(self):
        fake_db = self.database_mocker(BooleanModel,
                                       BOOLEAN_MODEL_TABLE_DATA,
                                       BOOLEAN_MODEL_FALSE_ITEM_DATA,
                                 'user_name', 'S',
                                 'alf')
        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = BOOLEAN_MODEL_FALSE_ITEM_DATA
            item = BooleanModel.get('alf')
            self.assertFalse(item.is_human)

    def test_deserializing_new_style_bool_true_works(self):
        fake_db = self.database_mocker(BooleanModel,
                                       BOOLEAN_MODEL_TABLE_DATA,
                                       BOOLEAN_MODEL_TRUE_ITEM_DATA,
                                 'user_name', 'S',
                                 'justin')
        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = BOOLEAN_MODEL_TRUE_ITEM_DATA
            item = BooleanModel.get('justin')
            self.assertTrue(item.is_human)

    def test_serializing_map_with_null_check(self):
        class TreeModelWithList(TreeModel):
            leaves = ListAttribute(of=TreeLeaf)

        item = TreeModelWithList(
            tree_key='test',
            left=TreeNode1(
                value=42,
                left=TreeNode2(
                    value=42,
                    left=TreeLeaf(value=42),
                    right=TreeLeaf(value=42),
                ),
                right=TreeNode2(
                    value=42,
                    left=TreeLeaf(value=42),
                    right=TreeLeaf(value=42),
                ),
            ),
            right=TreeNode1(
                value=42,
                left=TreeNode2(
                    value=42,
                    left=TreeLeaf(value=42),
                    right=TreeLeaf(value=42),
                ),
                right=TreeNode2(
                    value=42,
                    left=TreeLeaf(value=42),
                    right=TreeLeaf(value=42),
                ),
            ),
            leaves=[
                TreeLeaf(value=42),
            ],
        )
        item.serialize(null_check=True)

        # now let's nullify an attribute a few levels deep to test that `null_check` propagates
        item.left.left.left.value = None
        item.serialize(null_check=False)

        # now with null check
        with pytest.raises(Exception, match="Attribute 'left.left.left.value' cannot be None"):
            item.serialize(null_check=True)

        # now let's nullify an attribute of a map in a list to test that `null_check` propagates
        item.left.left.left.value = 42
        item.leaves[0].value = None
        item.serialize(null_check=False)

        # now with null check
        with pytest.raises(Exception, match=r"Attribute 'leaves.\[0\].value' cannot be None"):
            item.serialize(null_check=True)


    def test_deserializing_map_four_layers_deep_works(self):
        fake_db = self.database_mocker(TreeModel,
                                       TREE_MODEL_TABLE_DATA,
                                       TREE_MODEL_ITEM_DATA,
                                 'tree_key', 'S',
                                 '123')
        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = TREE_MODEL_ITEM_DATA
            item = TreeModel.get('123')
            self.assertEqual(item.left.left.left.value, 3)

    def test_explicit_raw_map_serialize_pass(self):
        map_native = {'foo': 'bar'}
        map_serialized = {'M': {'foo': {'S': 'bar'}}}
        instance = ExplicitRawMapModel(map_attr=map_native)
        serialized = instance.serialize()
        self.assertEqual(serialized['map_attr'], map_serialized)

    def test_raw_map_serialize_fun_one(self):
        map_native = {
            'foo': 'bar', 'num': 12345678909876543211234234324234, 'bool_type': True,
            'other_b_type': False, 'floaty': 1.2, 'listy': [1,2,3],
            'mapy': {'baz': 'bongo'}
        }
        expected = {'M': {'foo': {'S': u'bar'},
               'listy': {'L': [{'N': '1'}, {'N': '2'}, {'N': '3'}]},
               'num': {'N': '12345678909876543211234234324234'}, 'other_b_type': {'BOOL': False},
               'floaty': {'N': '1.2'}, 'mapy': {'M': {'baz': {'S': u'bongo'}}},
               'bool_type': {'BOOL': True}}}

        instance = ExplicitRawMapModel(map_attr=map_native)
        serialized = instance.serialize()
        actual = serialized['map_attr']
        self.assertEqual(expected, actual)

    def test_raw_map_deserializes(self):
        map_native = {
            'foo': 'bar', 'num': 1, 'bool_type': True,
            'other_b_type': False, 'floaty': 1.2, 'listy': [1, 2, 12345678909876543211234234324234],
            'mapy': {'baz': 'bongo'}
        }
        map_serialized = {
            'M': {
                'foo': {'S': 'bar'},
                'num': {'N': '1'},
                'bool_type': {'BOOL': True},
                'other_b_type': {'BOOL': False},
                'floaty': {'N': '1.2'},
                'listy': {'L': [{'N': '1'}, {'N': '2'}, {'N': '12345678909876543211234234324234'}]},
                'mapy': {'M': {'baz': {'S': 'bongo'}}}
            }
        }
        instance = ExplicitRawMapModel()
        instance.deserialize({'map_attr': map_serialized})
        actual = instance.map_attr
        for k, v in map_native.items():
            self.assertEqual(v, actual[k])
        self.assertEqual(
            repr(actual),
            "MapAttribute(foo='bar', num=1, bool_type=True, other_b_type=False, floaty=1.2, listy=[1, 2, 12345678909876543211234234324234], mapy={'baz': 'bongo'})"
        )

    def test_raw_map_from_raw_data_works(self):
        map_native = {
            'foo': 'bar', 'num': 1, 'bool_type': True,
            'other_b_type': False, 'floaty': 1.2, 'listy': [1, 2, 12345678909876543211234234324234],
            'mapy': {'baz': 'bongo'}
        }
        fake_db = self.database_mocker(ExplicitRawMapModel,
                                       EXPLICIT_RAW_MAP_MODEL_TABLE_DATA,
                                       EXPLICIT_RAW_MAP_MODEL_ITEM_DATA,
                                       'map_id', 'N',
                                       '123')
        with patch(PATCH_METHOD, new=fake_db):
            item = ExplicitRawMapModel.get(123)
            actual = item.map_attr
            self.assertEqual(map_native.get('listy')[2], actual['listy'][2])
            for k, v in map_native.items():
                self.assertEqual(v, actual[k])

    def test_raw_map_as_sub_map_serialize_pass(self):
        map_native = {'a': 'dict', 'lives': [123, 456], 'here': True}
        map_serialized = {
            'M': {
                'a': {'S': 'dict'},
                'lives': {'L': [{'N': '123'}, {'N': '456'}]},
                'here': {'BOOL': True}
            }
        }
        instance = ExplicitRawMapAsMemberOfSubClass(
            map_id=123,
            sub_attr=MapAttrSubClassWithRawMapAttr(
                num_field=37, str_field='hi',
                map_field=map_native
            )
        )
        serialized = instance.serialize()
        self.assertEqual(serialized['sub_attr']['M']['map_field'], map_serialized)

    def _get_raw_map_as_sub_map_test_data(self):
        map_native = {
            'foo': 'bar', 'num': 1, 'bool_type': True,
            'other_b_type': False, 'floaty': 1.2, 'listy': [1, 2, 3],
            'mapy': {'baz': 'bongo'}
        }
        map_serialized = {
            'M': {
                'foo': {'S': 'bar'},
                'num': {'N': '1'},
                'bool_type': {'BOOL': True},
                'other_b_type': {'BOOL': False},
                'floaty': {'N': '1.2'},
                'listy': {'L': [{'N': '1'}, {'N': '2'}, {'N': '3'}]},
                'mapy': {'M': {'baz': {'S': 'bongo'}}}
            }
        }

        sub_attr = MapAttrSubClassWithRawMapAttr(
            num_field=37, str_field='hi', map_field=map_native
        )

        instance = ExplicitRawMapAsMemberOfSubClass(
            map_id=123,
            sub_attr=sub_attr
        )
        return map_native, map_serialized, sub_attr, instance

    def test_raw_map_as_sub_map(self):
        map_native, map_serialized, sub_attr, instance = self._get_raw_map_as_sub_map_test_data()
        actual = instance.sub_attr
        self.assertEqual(sub_attr, actual)
        self.assertEqual(actual.map_field['floaty'], map_native.get('floaty'))
        self.assertEqual(actual.map_field['mapy']['baz'], map_native.get('mapy').get('baz'))

    def test_raw_map_as_sub_map_deserialize(self):
        map_native, map_serialized, _, _ = self._get_raw_map_as_sub_map_test_data()

        actual = MapAttrSubClassWithRawMapAttr().deserialize({
            "map_field": map_serialized
        })

        for k, v in map_native.items():
            self.assertEqual(actual.map_field[k], v)

    def test_raw_map_as_sub_map_from_raw_data_works(self):
        map_native, map_serialized, sub_attr, instance = self._get_raw_map_as_sub_map_test_data()
        fake_db = self.database_mocker(ExplicitRawMapAsMemberOfSubClass,
                                       EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_TABLE_DATA,
                                       EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_ITEM_DATA,
                                       'map_id', 'N',
                                       '123')
        with patch(PATCH_METHOD, new=fake_db):
            item = ExplicitRawMapAsMemberOfSubClass.get(123)
            actual = item.sub_attr
            self.assertEqual(sub_attr.map_field['floaty'],
                             map_native.get('floaty'))
            self.assertEqual(actual.map_field['mapy']['baz'],
                             map_native.get('mapy').get('baz'))

    def test_model_subclass_attributes_inherited_on_create(self):
        scope_args = {'count': 0}

        def fake_dynamodb(*args, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            return {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            Dog.create_table(read_capacity_units=2, write_capacity_units=2)

            actual = req.call_args_list[1][0][1]

            self.assertEqual(actual['TableName'], DOG_TABLE_DATA['Table']['TableName'])
            self.assert_dict_lists_equal(actual['KeySchema'], DOG_TABLE_DATA['Table']['KeySchema'])
            self.assert_dict_lists_equal(actual['AttributeDefinitions'],
                                         DOG_TABLE_DATA['Table']['AttributeDefinitions'])

    def test_model_version_attribute_save_with_initial_version_zero(self):
        item = VersionedModel('test_user_name', email='test_user@email.com', version=0)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            item.save()
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'name': {
                        'S': 'test_user_name'
                    },
                    'email': {
                        'S': 'test_user@email.com'
                    },
                    'version': {
                        'N': '1'
                    },
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'VersionedModel',
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {'#0': 'version'},
                'ExpressionAttributeValues': {':0': {'N': '0'}}
            }

            deep_eq(args, params, _assert=True)
            item.version = 1
            item.name = "test_new_username"
            item.save()
            args = req.call_args[0][1]

            params = {
                'Item': {
                    'name': {
                        'S': 'test_new_username'
                    },
                    'email': {
                        'S': 'test_user@email.com'
                    },
                    'version': {
                        'N': '2'
                    },
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'VersionedModel',
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {'#0': 'version'},
                'ExpressionAttributeValues': {':0': {'N': '1'}}
            }

            deep_eq(args, params, _assert=True)


class ModelInitTestCase(TestCase):

    def test_raw_map_attribute_with_dict_init(self):
        attribute = {
            'foo': 123,
            'bar': 'baz'
        }
        actual = ExplicitRawMapModel(map_id=3, map_attr=attribute)
        self.assertEqual(actual.map_attr['foo'], attribute['foo'])

    def test_raw_map_attribute_with_initialized_instance_init(self):
        attribute = {
            'foo': 123,
            'bar': 'baz'
        }
        initialized_instance = MapAttribute(**attribute)
        actual = ExplicitRawMapModel(map_id=3, map_attr=initialized_instance)
        self.assertEqual(actual.map_attr['foo'], initialized_instance['foo'])
        self.assertEqual(actual.map_attr['foo'], attribute['foo'])

    def test_subclassed_map_attribute_with_dict_init(self):
        attribute = {
            'make': 'Volkswagen',
            'model': 'Super Beetle'
        }
        expected_model = CarInfoMap(**attribute)
        actual = CarModel(car_id=1, car_info=attribute)
        self.assertEqual(expected_model.make, actual.car_info.make)
        self.assertEqual(expected_model.model, actual.car_info.model)

    def test_subclassed_map_attribute_with_initialized_instance_init(self):
        attribute = {
            'make': 'Volkswagen',
            'model': 'Super Beetle'
        }
        expected_model = CarInfoMap(**attribute)
        actual = CarModel(car_id=1, car_info=expected_model)
        self.assertEqual(expected_model.make, actual.car_info.make)
        self.assertEqual(expected_model.model, actual.car_info.model)

    def _get_bin_tree(self, multiplier=1):
        return {
            'value': 5 * multiplier,
            'left': {
                'value': 2 * multiplier,
                'left': {
                    'value': 1 * multiplier
                },
                'right': {
                    'value': 3 * multiplier
                }
            },
            'right': {
                'value': 7 * multiplier,
                'left': {
                    'value': 6 * multiplier
                },
                'right': {
                    'value': 8 * multiplier
                }
            }
        }

    def test_subclassed_map_attribute_with_map_attributes_member_with_dict_init(self):
        left = self._get_bin_tree()
        right = self._get_bin_tree(multiplier=2)
        actual = TreeModel(tree_key='key', left=left, right=right)
        self.assertEqual(actual.left.left.right.value, 3)
        self.assertEqual(actual.left.left.value, 2)
        self.assertEqual(actual.right.right.left.value, 12)
        self.assertEqual(actual.right.right.value, 14)

    def test_subclassed_map_attribute_with_map_attribute_member_with_initialized_instance_init(self):
        left = self._get_bin_tree()
        right = self._get_bin_tree(multiplier=2)
        left_instance = TreeNode1(**left)
        right_instance = TreeNode1(**right)
        actual = TreeModel(tree_key='key', left=left_instance, right=right_instance)
        self.assertEqual(actual.left.left.right.value, left_instance.left.right.value)
        self.assertEqual(actual.left.left.value, left_instance.left.value)
        self.assertEqual(actual.right.right.left.value, right_instance.right.left.value)
        self.assertEqual(actual.right.right.value, right_instance.right.value)

    def test_multiple_ttl_attributes(self):
        with self.assertRaises(ValueError):
            class BadTTLModel(Model):
                class Meta:
                    table_name = 'BadTTLModel'
                ttl = TTLAttribute(default_for_new=timedelta(minutes=1))
                another_ttl = TTLAttribute()

    def test_get_ttl_attribute_fails(self):
        with patch(PATCH_METHOD) as req:
            req.side_effect = Exception
            self.assertRaises(Exception, TTLModel.update_ttl, False)

    def test_get_ttl_attribute(self):
        assert TTLModel._ttl_attribute().attr_name == "my_ttl"

    def test_deserialized(self):
        m = TTLModel.from_raw_data({'user_name': {'S': 'mock'}})
        assert m.my_ttl is None

    def test_deserialized_with_ttl(self):
        m = TTLModel.from_raw_data({'user_name': {'S': 'mock'}, 'my_ttl': {'N': '1546300800'}})
        assert m.my_ttl == datetime(2019, 1, 1, tzinfo=timezone.utc)

    def test_deserialized_with_invalid_type(self):
        self.assertRaises(AttributeDeserializationError, TTLModel.from_raw_data, {'my_ttl': {'S': '1546300800'}})

    def test_multiple_hash_keys(self):
        with self.assertRaises(ValueError):
            class BadHashKeyModel(Model):
                class Meta:
                    table_name = 'BadHashKeyModel'

                foo = UnicodeAttribute(hash_key=True)
                bar = UnicodeAttribute(hash_key=True)

    def test_multiple_range_keys(self):
        with self.assertRaises(ValueError):
            class BadRangeKeyModel(Model):
                class Meta:
                    table_name = 'BadRangeKeyModel'

                foo = UnicodeAttribute(range_key=True)
                bar = UnicodeAttribute(range_key=True)

    def test_multiple_version_attributes(self):
        with self.assertRaises(ValueError):
            class BadVersionedModel(Model):
                class Meta:
                    table_name = 'BadVersionedModel'

                version = VersionAttribute()
                another_version = VersionAttribute()

    def test_inherit_metaclass(self):
        class ParentModel(Model):
            class Meta:
                table_name = 'foo'
        class ChildModel(ParentModel):
            pass
        self.assertEqual(ParentModel.Meta.table_name, ChildModel.Meta.table_name)

    def test_connection_inheritance(self):
        class Foo(Model):
            class Meta:
                table_name = 'foo'
        class Bar(Foo):
            class Meta:
                table_name = 'bar'
        class Baz(Foo):
            pass
        assert Foo._get_connection() is not Bar._get_connection()
        assert Foo._get_connection() is Baz._get_connection()
        self.assertEqual(Foo._get_connection().table_name, Foo.Meta.table_name)
        self.assertEqual(Bar._get_connection().table_name, Bar.Meta.table_name)
        self.assertEqual(Baz._get_connection().table_name, Baz.Meta.table_name)


@pytest.mark.parametrize('add_version_condition', [True, False])
def test_model_version_attribute_save(add_version_condition: bool) -> None:
    item = VersionedModel('test_user_name', email='test_user@email.com')
    with patch(PATCH_METHOD) as req:
        req.return_value = {}
        item.save(add_version_condition=add_version_condition)
        args = req.call_args[0][1]
        params = {
            'Item': {
                'name': {
                    'S': 'test_user_name'
                },
                'email': {
                    'S': 'test_user@email.com'
                },
                'version': {
                    'N': '1'
                },
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'TableName': 'VersionedModel',
        }
        if add_version_condition:
            params.update({
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {'#0': 'version'},
            })

        assert args == params
        deep_eq(args, params, _assert=True)
        item.version = 1
        item.name = "test_new_username"
        item.save(add_version_condition=add_version_condition)
        args = req.call_args[0][1]

        params = {
            'Item': {
                'name': {
                    'S': 'test_new_username'
                },
                'email': {
                    'S': 'test_user@email.com'
                },
                'version': {
                    'N': '2'
                },
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'TableName': 'VersionedModel',
        }
        if add_version_condition:
            params.update({
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {'#0': 'version'},
                'ExpressionAttributeValues': {':0': {'N': '1'}}
            })

        assert args == params


@pytest.mark.parametrize('add_version_condition', [True, False])
def test_version_attribute_increments_on_update(add_version_condition: bool) -> None:
    item = VersionedModel('test_user_name', email='test_user@email.com')

    with patch(PATCH_METHOD) as req:
        req.return_value = {
            ATTRIBUTES: {
                'name': {
                    'S': 'test_user_name'
                },
                'email': {
                    'S': 'new@email.com'
                },
                'version': {
                    'N': '1'
                },
            }
        }
        item.update(actions=[VersionedModel.email.set('new@email.com')], add_version_condition=add_version_condition)
        args = req.call_args[0][1]
        expected = {
            'ExpressionAttributeValues': {
                ':0': {
                    'S': 'new@email.com'
                },
                ':1': {
                    'N': '1'
                }
            },
            'Key': {
                'name': {
                    'S': 'test_user_name'
                }
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'ReturnValues': 'ALL_NEW',
            'TableName': 'VersionedModel',
        }
        if add_version_condition:
            expected.update({
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {'#0': 'version', '#1': 'email'},
                'UpdateExpression': 'SET #1 = :0, #0 = :1',
            })
        else:
            expected.update({
                'ExpressionAttributeNames': {'#0': 'email', '#1': 'version'},
                'UpdateExpression': 'SET #0 = :0, #1 = :1',
            })

        assert args == expected
        assert item.version == 1

    with patch(PATCH_METHOD) as req:
        req.return_value = {
            ATTRIBUTES: {
                'name': {
                    'S': 'test_user_name'
                },
                'email': {
                    'S': 'newer@email.com'
                },
                'version': {
                    'N': '2'
                },
            }
        }

        item.update(actions=[VersionedModel.email.set('newer@email.com')], add_version_condition=add_version_condition)
        args = req.call_args[0][1]
        expected = {
            'Key': {
                'name': {
                    'S': 'test_user_name'
                }
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'ReturnValues': 'ALL_NEW',
            'TableName': 'VersionedModel',
        }
        if add_version_condition:
            expected.update({
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {'#0': 'version', '#1': 'email'},
                'ExpressionAttributeValues': {':0': {'N': '1'}, ':1': {'S': 'newer@email.com'}, ':2': {'N': '1'}},
                'UpdateExpression': 'SET #1 = :1 ADD #0 :2',
            })
        else:
            expected.update({
                'ExpressionAttributeValues': {':0': {'S': 'newer@email.com'}, ':1': {'N': '1'}},
                'ExpressionAttributeNames': {'#0': 'email', '#1': 'version'},
                'UpdateExpression': 'SET #0 = :0 ADD #1 :1',
            })

        assert args == expected


@pytest.mark.parametrize('add_version_condition', [True, False])
def test_delete(add_version_condition: bool) -> None:
    item = UserModel('foo', 'bar')

    with patch(PATCH_METHOD) as req:
        req.return_value = None
        item.delete(add_version_condition=add_version_condition)
        expected = {
            'Key': {
                'user_id': {
                    'S': 'bar'
                },
                'user_name': {
                    'S': 'foo'
                }
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'TableName': 'UserModel'
        }
        args = req.call_args[0][1]
        assert args == expected

    with patch(PATCH_METHOD) as req:
        req.return_value = None
        item.delete(UserModel.user_id =='bar', add_version_condition=add_version_condition)
        expected = {
            'Key': {
                'user_id': {
                    'S': 'bar'
                },
                'user_name': {
                    'S': 'foo'
                }
            },
            'ConditionExpression': '#0 = :0',
            'ExpressionAttributeNames': {
                '#0': 'user_id'
            },
            'ExpressionAttributeValues': {
                ':0': {
                    'S': 'bar'
                }
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'TableName': 'UserModel'
        }
        args = req.call_args[0][1]
        assert args == expected

    with patch(PATCH_METHOD) as req:
        req.return_value = None
        item.delete(UserModel.user_id == 'bar', add_version_condition=add_version_condition)
        expected = {
            'Key': {
                'user_id': {
                    'S': 'bar'
                },
                'user_name': {
                    'S': 'foo'
                }
            },
            'ConditionExpression': '#0 = :0',
            'ExpressionAttributeNames': {
                '#0': 'user_id'
            },
            'ExpressionAttributeValues': {
                ':0': {
                    'S': 'bar'
                }
            },
            'ReturnConsumedCapacity': 'TOTAL',
            'TableName': 'UserModel'
        }
        args = req.call_args[0][1]
        assert args == expected

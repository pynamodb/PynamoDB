"""
Test model API
"""
from pynamodb.models import Model
from pynamodb.connection import Connection
from pynamodb.attributes import UnicodeAttribute, NumberAttribute, BinaryAttribute
from unittest import TestCase
from .response import HttpOK
from .data import MODEL_TABLE_DATA, GET_MODEL_ITEM_DATA

# Py2/3
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

PATCH_METHOD = 'botocore.operation.Operation.call'


class UserModel(Model):
    """
    A testing model
    """
    table_name = 'UserModel'
    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)
    picture = BinaryAttribute()
    zip_code = NumberAttribute()
    email = UnicodeAttribute()


class ModelTestCase(TestCase):
    """
    Tests for the models API
    """
    def assertDictListsEqual(self, list1, list2):
        """
        Compares two lists of dictionariess
        """
        for d1_item in list1:
            found = False
            for d2_item in list2:
                if d2_item.items() == d1_item.items():
                    found = True
            if not found:
                raise AssertionError("Values not equal: {0} {1}".format(d1_item, list2))

    def test_create_model(self):
        """
        Model.create_table
        """
        self.maxDiff = None
        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), None
            UserModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = {
                'table_name': 'UserModel',
                'attribute_definitions': [
                    {
                        'AttributeName': 'user_name', 'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'user_id', 'AttributeType': 'S'
                    },
                ],
                'provisioned_throughput': {
                    'ReadCapacityUnits': 2, 'WriteCapacityUnits': 2
                },
                'key_schema': [
                    {
                        'AttributeName': 'user_name', 'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'user_id', 'KeyType': 'RANGE'
                    }
                ]
            }
            params = {item: value for item, value in iter(sorted(params.items()))}
            mock_params = req.call_args[1]
            self.assertEqual(params['provisioned_throughput'], mock_params['provisioned_throughput'])
            self.assertEqual(params['table_name'], mock_params['table_name'])
            self.assertDictListsEqual(params['attribute_definitions'], mock_params['attribute_definitions'])
            self.assertDictListsEqual(params['key_schema'], mock_params['key_schema'])

    def test_get(self):
        """
        Model.get
        """
        conn = Connection()
        with patch(PATCH_METHOD) as req:
            conn.describe_table('Thread')
            req.return_value = HttpOK(), MODEL_TABLE_DATA

        with patch(PATCH_METHOD) as req:
            req.return_value = HttpOK(), GET_MODEL_ITEM_DATA
            UserModel.get(
                'foo',
                'bar'
            )
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

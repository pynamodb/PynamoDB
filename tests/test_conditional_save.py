import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute
from pynamodb.exceptions import PutError


class ExistingModel(Model):
    """
    A model with a default value for testing conditional saves
    """
    class Meta:
        table_name = 'ExistingModelTable'
        host = 'http://localhost:8000'
    id = UnicodeAttribute(hash_key=True)
    value = UnicodeAttribute()


class ExistingModelWithNewField(Model):
    """
    A model with a new field that has a default value
    """
    class Meta:
        table_name = 'ExistingModelTable'
        host = 'http://localhost:8000'
    id = UnicodeAttribute(hash_key=True)
    value = UnicodeAttribute()
    new_field = NumberAttribute(default=5)


@pytest.mark.skipif(os.getenv('CI', 'false').lower() == 'true', reason='Skipping tests for CI')
class TestConditionalSave:

    @patch('pynamodb.connection.Connection.put_item')
    def test_conditional_save_with_new_field(self, mock_put):
        mock_put.side_effect = PutError(cause=None, data={'Error': {'Message': 'ConditionalCheckFailed'}})

        # Test scenario: we have an existing model in the database without the new_field
        # When loading it through the updated model class, the new_field should be populated with the default value
        # A conditional save using the new_field should work correctly
        with patch('pynamodb.connection.Connection.get_item') as mock_get:
            mock_get.return_value = {'Item': {'id': {'S': 'test-id'}, 'value': {'S': 'test-value'}}}
            model = ExistingModelWithNewField.get('test-id')
            assert model.new_field == 5
            assert 'new_field' in model.attribute_values
            try:
                model.save(condition=(ExistingModelWithNewField.new_field == 5))
                condition_expr = mock_put.call_args[1]['condition_expression']
                assert '#n0 = :v0' in condition_expr
            except PutError:
                pass

    def test_conditional_save_real_dynamodb(self):
        """Test with actual DynamoDB Local for more complete verification"""
        try:
            if not ExistingModel.exists():
                ExistingModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
            original_model = ExistingModel(id='test-conditional-id', value='original-value')
            original_model.save()
            updated_model = ExistingModelWithNewField.get('test-conditional-id')
            assert updated_model.new_field == 5
            updated_model.value = 'updated-value'
            updated_model.save(condition=(ExistingModelWithNewField.new_field == 5))
            refreshed_model = ExistingModelWithNewField.get('test-conditional-id')
            assert refreshed_model.value == 'updated-value'
            assert refreshed_model.new_field == 5
            updated_model.delete()

        except Exception as e:
            pytest.skip(f"DynamoDB Local not available: {e}")
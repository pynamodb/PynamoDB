import os
import pytest

from pynamodb.attributes import BooleanAttribute, LegacyBooleanAttribute, UnicodeAttribute
from pynamodb.expressions.operand import Path
from pynamodb.migration import migrate_boolean_attributes
from pynamodb.models import Model


@pytest.fixture()
def ddb_url():
    """Obtain the URL of a local DynamoDB instance.

    This is meant to be used with something like DynamoDB Local:

      http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html

    It must be set up "out of band"; we merely assume it exists on
    http://localhost:8000 or a URL specified though the
    PYNAMODB_INTEGRATION_TEST_DDB_URL environment variable.
    """
    ddb_url = os.getenv("PYNAMODB_INTEGRATION_TEST_DDB_URL")
    return "http://localhost:8000" if ddb_url is None else ddb_url


@pytest.mark.ddblocal
def test_migrate_boolean_attributes_upgrade_path(ddb_url):
    class BAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = BooleanAttribute(null=True)

    class LBAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)

    LBAModel.create_table(read_capacity_units=1, write_capacity_units=1)

    # Create one "offending" object written as an integer using LBA.
    LBAModel('pkey', flag=True).save()
    assert 1 == len([_ for _ in LBAModel.query('pkey', LBAModel.flag == True)])

    # We should NOT be able to read it using BA.
    assert 0 == len([_ for _ in BAModel.query('pkey', BAModel.flag == True)])

    # ... unless we jump through hoops using Path
    assert 1 == len([_ for _ in BAModel.query('pkey', Path('flag') == 1)])

    # Migrate the object to being stored as Boolean.
    assert (1, 0) == migrate_boolean_attributes(BAModel, ['flag'], allow_rate_limited_scan_without_consumed_capacity=True)

    # We should now be able to read it using BA.
    assert 1 == len([_ for _ in BAModel.query('pkey', BAModel.flag == True)])

    # ... or through the hoop jumping.
    assert 1 == len([_ for _ in BAModel.query('pkey', Path('flag') == True)])


@pytest.mark.ddblocal
def test_migrate_boolean_attributes_none_okay(ddb_url):
    """Ensure migration works for attributes whose value is None."""
    class LBAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)

    LBAModel.create_table(read_capacity_units=1, write_capacity_units=1)
    LBAModel('pkey', flag=None).save()
    assert (0, 0) == migrate_boolean_attributes(LBAModel, ['flag'], allow_rate_limited_scan_without_consumed_capacity=True)


@pytest.mark.ddblocal
def test_migrate_boolean_attributes_conditional_update_failure(ddb_url):
    """Ensure migration works for attributes whose value is None."""
    class LBAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)

    LBAModel.create_table(read_capacity_units=1, write_capacity_units=1)
    LBAModel('pkey', flag=1).save()
    assert (1, 1) == migrate_boolean_attributes(LBAModel, ['flag'],
                                                allow_rate_limited_scan_without_consumed_capacity=True,
                                                mock_conditional_update_failure=True)


@pytest.mark.ddblocal
def test_migrate_boolean_attributes_missing_attribute(ddb_url):
    class LBAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)

    LBAModel.create_table(read_capacity_units=1, write_capacity_units=1)
    LBAModel('pkey', flag=True).save()
    with pytest.raises(ValueError) as e:
        migrate_boolean_attributes(LBAModel, ['flag', 'bogus'], allow_rate_limited_scan_without_consumed_capacity=True)
    assert str(e.value).find('does not exist on model') != -1


@pytest.mark.ddblocal
def test_migrate_boolean_attributes_wrong_attribute_type(ddb_url):
    class LBAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)
        other = UnicodeAttribute(null=True)

    LBAModel.create_table(read_capacity_units=1, write_capacity_units=1)
    LBAModel('pkey', flag=True, other='test').save()
    with pytest.raises(ValueError) as e:
        migrate_boolean_attributes(LBAModel, ['flag', 'other'], allow_rate_limited_scan_without_consumed_capacity=True)
    assert str(e.value).find('does not appear to be a boolean attribute') != -1


@pytest.mark.ddblocal
def test_migrate_boolean_attributes_multiple_attributes(ddb_url):
    class LBAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)
        flag2 = LegacyBooleanAttribute(null=True)

    LBAModel.create_table(read_capacity_units=1, write_capacity_units=1)
    # specifically use None and True here rather than two Trues
    LBAModel('pkey', flag=None, flag2=True).save()
    assert (1, 0) == migrate_boolean_attributes(LBAModel, ['flag', 'flag2'], allow_rate_limited_scan_without_consumed_capacity=True)


@pytest.mark.ddblocal
def test_migrate_boolean_attributes_skip_native_booleans(ddb_url):
    class BAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = BooleanAttribute(null=True)

    BAModel.create_table(read_capacity_units=1, write_capacity_units=1)
    BAModel('pkey', flag=True).save()
    assert (0, 0) == migrate_boolean_attributes(BAModel, ['flag'], allow_rate_limited_scan_without_consumed_capacity=True)


@pytest.mark.parametrize("flag_value",[True, False, None])
@pytest.mark.ddblocal
def test_legacy_boolean_attribute_deserialization_in_update(ddb_url, flag_value):
    class BAModel(Model):
        class Meta:
            table_name = 'lba_deserialization_test'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = BooleanAttribute(null=True)
        value = UnicodeAttribute(null=True)

    class LBAModel(Model):
        class Meta:
            table_name = 'lba_deserialization_test'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)
        value = UnicodeAttribute(null=True)

    BAModel.create_table(read_capacity_units=1, write_capacity_units=1)

    # Create objects with a BooleanAttribute flag
    BAModel('pkey', flag=flag_value, value = 'value').save()

    # Check we are able to read the flag with LegacyBooleanAttribute
    assert flag_value == LBAModel.get('pkey').flag

    # Update a value in the model causing LegacyBooleanAttribute to be deserialized
    LBAModel.get('pkey').update(actions=[LBAModel.value.set('new value')])

    # Check we are able to read the flag with LegacyBooleanAttribute
    assert flag_value == LBAModel.get('pkey').flag


@pytest.mark.parametrize("flag_value",[True, False, None])
@pytest.mark.ddblocal
def test_legacy_boolean_attribute_deserialization_in_update_item(ddb_url, flag_value):
    class BAModel(Model):
        class Meta:
            table_name = 'lba_deserialization_test'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = BooleanAttribute(null=True)
        value = UnicodeAttribute(null=True)

    class LBAModel(Model):
        class Meta:
            table_name = 'lba_deserialization_test'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)
        value = UnicodeAttribute(null=True)

    BAModel.create_table(read_capacity_units=1, write_capacity_units=1)

    # Create objects with a BooleanAttribute flag
    BAModel('pkey', flag=flag_value, value = 'value').save()

    # Check we are able to read the flag with LegacyBooleanAttribute
    assert flag_value == LBAModel.get('pkey').flag

    # Update a value in the model causing LegacyBooleanAttribute to be deserialized
    LBAModel.get('pkey').update_item('value', 'new value', 'PUT')

    # Check we are able to read the flag with LegacyBooleanAttribute
    assert flag_value == LBAModel.get('pkey').flag


@pytest.mark.parametrize("flag_value",[True, False, None])
@pytest.mark.ddblocal
def test_boolean_attribute_deserialization_in_update(ddb_url, flag_value):
    class BAModel(Model):
        class Meta:
            table_name = 'ba_deserialization_test'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = BooleanAttribute(null=True)
        value = UnicodeAttribute(null=True)

    class LBAModel(Model):
        class Meta:
            table_name = 'ba_deserialization_test'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)
        value = UnicodeAttribute(null=True)

    LBAModel.create_table(read_capacity_units=1, write_capacity_units=1)

    # Create an object with a LegacyBooleanAttribute flag
    LBAModel('pkey', flag=flag_value, value = 'value').save()

    # Check we are able to read the flag with BooleanAttribute
    assert flag_value == BAModel.get('pkey').flag

    # Update a value in the model causing BooleanAttribute to be deserialized
    BAModel.get('pkey').update(actions=[BAModel.value.set('new value')])

    # Check we are able to read the flag with BooleanAttribute
    assert flag_value == BAModel.get('pkey').flag


@pytest.mark.parametrize("flag_value",[True, False, None])
@pytest.mark.ddblocal
def test_boolean_attribute_deserialization_in_update_item(ddb_url, flag_value):
    class BAModel(Model):
        class Meta:
            table_name = 'ba_deserialization_test'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = BooleanAttribute(null=True)
        value = UnicodeAttribute(null=True)

    class LBAModel(Model):
        class Meta:
            table_name = 'ba_deserialization_test'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = LegacyBooleanAttribute(null=True)
        value = UnicodeAttribute(null=True)

    LBAModel.create_table(read_capacity_units=1, write_capacity_units=1)

    # Create an object with a LegacyBooleanAttribute flag
    LBAModel('pkey', flag=flag_value, value = 'value').save()

    # Check we are able to read the flag with BooleanAttribute
    assert flag_value == BAModel.get('pkey').flag

    # Update a value in the model causing BooleanAttribute to be deserialized
    BAModel.get('pkey').update_item('value', 'new value', 'PUT')

    # Check we are able to read the flag with BooleanAttribute
    assert flag_value == BAModel.get('pkey').flag

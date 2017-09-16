import pytest

from datetime import datetime
from pynamodb.attributes import (BooleanAttribute, LegacyBooleanAttribute,
                                 UTCDateTimeAttribute, LegacyUTCDateTimeAttribute,
                                 UnicodeAttribute)
from pynamodb.expressions.operand import Path
from pynamodb.migration import migrate_boolean_attributes, migrate_datetime_attributes
from pynamodb.models import Model

import logging
logging.basicConfig(level='INFO')


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

    LBAModel.delete_table()


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

    LBAModel.delete_table()


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

    LBAModel.delete_table()


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

    LBAModel.delete_table()


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

    LBAModel.delete_table()


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

    LBAModel.delete_table()


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
    BAModel.delete_table()


@pytest.mark.ddblocal
def test_migrate_datetime_attributes_upgrade_path(ddb_url):
    class DTModel(Model):
        class Meta:
            table_name = 'migration_test_ldt_to_dt'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        timestamp = UTCDateTimeAttribute(null=True)

    class LDTModel(Model):
        class Meta:
            table_name = 'migration_test_ldt_to_dt'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        timestamp = LegacyUTCDateTimeAttribute(null=True)

    LDTModel.create_table(read_capacity_units=1, write_capacity_units=1)

    # Create one "offending" object written as a string using LDT.
    test_dt_old = '2009-02-13T23:31:30.123456+0000'
    test_dt_new = 1234567890.123456
    test_dt = datetime.utcfromtimestamp(test_dt_new)
    LDTModel('pkey', timestamp=test_dt).save()
    assert 1 == len([_ for _ in LDTModel.query('pkey', LDTModel.timestamp == test_dt)])

    # We should NOT be able to read it using DT.
    assert 0 == len([_ for _ in DTModel.query('pkey', DTModel.timestamp == test_dt)])

    # ... unless we jump through hoops using Path
    assert 1 == len([_ for _ in DTModel.query('pkey', Path('timestamp') == test_dt_old)])

    # Migrate the object to being stored as Number
    assert (1, 0) == migrate_datetime_attributes(DTModel, ['timestamp'], allow_rate_limited_scan_without_consumed_capacity=True)

    # We should now be able to read it using DT.
    assert 1 == len([_ for _ in DTModel.query('pkey', DTModel.timestamp == test_dt)])

    # ... or through the hoop jumping.
    assert 1 == len([_ for _ in DTModel.query('pkey', Path('timestamp') == test_dt_new)])

    LDTModel.delete_table()


@pytest.mark.ddblocal
def test_migrate_datetime_attributes_none_okay(ddb_url):
    """Ensure migration works for attributes whose value is None."""
    class LDTModel(Model):
        class Meta:
            table_name = 'migration_test_ldt_to_dt'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        timestamp = LegacyUTCDateTimeAttribute(null=True)

    LDTModel.create_table(read_capacity_units=1, write_capacity_units=1)
    LDTModel('pkey', timestamp=None).save()
    assert (0, 0) == migrate_datetime_attributes(LDTModel, ['timestamp'], allow_rate_limited_scan_without_consumed_capacity=True)

    LDTModel.delete_table()


@pytest.mark.ddblocal
def test_migrate_datetime_attributes_conditional_update_failure(ddb_url):
    """Ensure migration works for attributes whose value is None."""
    class LDTModel(Model):
        class Meta:
            table_name = 'migration_test_ldt_to_dt'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        timestamp = LegacyUTCDateTimeAttribute(null=True)

    LDTModel.create_table(read_capacity_units=1, write_capacity_units=1)
    LDTModel('pkey', timestamp=datetime.utcfromtimestamp(1234567890.123456)).save()
    assert (1, 1) == migrate_datetime_attributes(LDTModel, ['timestamp'],
                                                allow_rate_limited_scan_without_consumed_capacity=True,
                                                mock_conditional_update_failure=True)

    LDTModel.delete_table()


@pytest.mark.ddblocal
def test_migrate_datetime_attributes_missing_attribute(ddb_url):
    class LDTModel(Model):
        class Meta:
            table_name = 'migration_test_ldt_to_dt'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        timestamp = LegacyUTCDateTimeAttribute(null=True)

    LDTModel.create_table(read_capacity_units=1, write_capacity_units=1)
    LDTModel('pkey', timestamp=datetime.utcfromtimestamp(1234567890.123456)).save()
    with pytest.raises(ValueError) as e:
        migrate_datetime_attributes(LDTModel, ['timestamp', 'bogus'], allow_rate_limited_scan_without_consumed_capacity=True)
    assert str(e.value).find('does not exist on model') != -1

    LDTModel.delete_table()


@pytest.mark.ddblocal
def test_migrate_datetime_attributes_wrong_attribute_type(ddb_url):
    class LDTModel(Model):
        class Meta:
            table_name = 'migration_test_ldt_to_dt'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        timestamp = LegacyUTCDateTimeAttribute(null=True)
        other = UnicodeAttribute(null=True)

    LDTModel.create_table(read_capacity_units=1, write_capacity_units=1)
    LDTModel('pkey', timestamp=datetime.utcfromtimestamp(1234567890.123456), other='test').save()
    with pytest.raises(ValueError) as e:
        migrate_datetime_attributes(LDTModel, ['timestamp', 'other'], allow_rate_limited_scan_without_consumed_capacity=True)
    assert str(e.value).find('does not appear to be a datetime attribute') != -1

    LDTModel.delete_table()


@pytest.mark.ddblocal
def test_migrate_datetime_attributes_multiple_attributes(ddb_url):
    class LDTModel(Model):
        class Meta:
            table_name = 'migration_test_ldt_to_dt'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        timestamp = LegacyUTCDateTimeAttribute(null=True)
        timestamp2 = LegacyUTCDateTimeAttribute(null=True)

    LDTModel.create_table(read_capacity_units=1, write_capacity_units=1)
    # specifically use None and True here rather than two Trues
    LDTModel('pkey', timestamp=None, timestamp2=datetime.utcfromtimestamp(1234567890.123456)).save()
    assert (1, 0) == migrate_datetime_attributes(LDTModel, ['timestamp', 'timestamp2'], allow_rate_limited_scan_without_consumed_capacity=True)

    LDTModel.delete_table()


@pytest.mark.ddblocal
def test_migrate_datetime_attributes_skip_numbers(ddb_url):
    class DTModel(Model):
        class Meta:
            table_name = 'migration_test_ldt_to_dt'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        timestamp = UTCDateTimeAttribute(null=True)

    DTModel.create_table(read_capacity_units=1, write_capacity_units=1)
    DTModel('pkey', timestamp=datetime.utcfromtimestamp(1234567890.123456)).save()
    assert (0, 0) == migrate_datetime_attributes(DTModel, ['timestamp'], allow_rate_limited_scan_without_consumed_capacity=True)

    DTModel.delete_table()

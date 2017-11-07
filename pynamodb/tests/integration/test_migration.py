import pytest

from pynamodb.attributes import BooleanAttribute, LegacyBooleanAttribute, UnicodeAttribute
from pynamodb.expressions.operand import Path
from pynamodb.migration import migrate_boolean_attributes
from pynamodb.models import Model


@pytest.fixture()
def ddb_url():
    # TODO: proper solution rather than assuming it's running on localhost:8000
    return "http://localhost:8000"


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
    assert 1 == migrate_boolean_attributes(BAModel, ['flag'], unit_testing=True)

    # We should now be able to read it using BA.
    assert 1 == len([_ for _ in BAModel.query('pkey', BAModel.flag == True)])

    # ... or through the hoop jumping.
    assert 1 == len([_ for _ in BAModel.query('pkey', Path('flag') == True)])


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
    assert 0 == migrate_boolean_attributes(LBAModel, ['flag'], unit_testing=True)


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
        migrate_boolean_attributes(LBAModel, ['flag', 'bogus'], unit_testing=True)
    assert e.value.message.find('does not exist on model') != -1


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
        migrate_boolean_attributes(LBAModel, ['flag', 'other'], unit_testing=True)
    assert e.value.message.find('does not appear to be a boolean attribute') != -1


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
    assert 1 == migrate_boolean_attributes(LBAModel, ['flag', 'flag2'], unit_testing=True)


def test_migrate_boolean_attributes_skip_native_booleans(ddb_url):
    class BAModel(Model):
        class Meta:
            table_name = 'migration_test_lba_to_ba'
            host = ddb_url
        id = UnicodeAttribute(hash_key=True)
        flag = BooleanAttribute(null=True)

    BAModel.create_table(read_capacity_units=1, write_capacity_units=1)
    BAModel('pkey', flag=True).save()
    assert 0 == migrate_boolean_attributes(BAModel, ['flag'], unit_testing=True)

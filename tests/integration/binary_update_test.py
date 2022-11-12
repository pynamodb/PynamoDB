import pytest

from pynamodb.attributes import LegacyBinaryAttribute
from pynamodb.attributes import LegacyBinarySetAttribute
from pynamodb.attributes import UnicodeAttribute, BinaryDataAttribute, BinaryDataSetAttribute
from pynamodb.models import Model


@pytest.mark.ddblocal
def test_binary_attribute_update(ddb_url):
    class DataModel(Model):
        class Meta:
            table_name = 'binary_attr_update'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinaryDataAttribute()
        legacy_data = LegacyBinaryAttribute()

    DataModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    data = b'\x00hey\xfb'
    pkey = 'pkey'
    DataModel(pkey, data=data, legacy_data=data).save()
    m = DataModel.get(pkey)
    assert m.data == data
    assert m.legacy_data == data

    new_data = b'\xff'
    m.update(actions=[
        DataModel.data.set(new_data),
        DataModel.legacy_data.set(new_data),
    ])
    assert m.data == new_data
    assert m.legacy_data == new_data


@pytest.mark.ddblocal
def test_binary_attribute_size(ddb_url):
    class OldAttrModel(Model):
        class Meta:
            table_name = 'binary_attr_old'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = LegacyBinaryAttribute()

    class NewAttrModel(Model):
        class Meta:
            table_name = 'binary_attr_new'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinaryDataAttribute()

    OldAttrModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    NewAttrModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

    old_model = OldAttrModel('pkey', data=b'spam' * 1000)
    old_rcu = old_model.save()['ConsumedCapacity']['CapacityUnits']

    new_model = NewAttrModel('pkey', data=b'spam' * 1000)
    new_rcu = new_model.save()['ConsumedCapacity']['CapacityUnits']

    assert old_rcu == 6
    assert new_rcu == 4


@pytest.mark.ddblocal
def test_binary_set_attribute_size(ddb_url):
    class OldAttrModel(Model):
        class Meta:
            table_name = 'binary_set_attr_old'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = LegacyBinarySetAttribute()

    class NewAttrModel(Model):
        class Meta:
            table_name = 'binary_set_attr_new'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinaryDataSetAttribute()

    OldAttrModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    NewAttrModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

    old_model = OldAttrModel('pkey', data={b'spam' * 1000})
    old_rcu = old_model.save()['ConsumedCapacity']['CapacityUnits']

    new_model = NewAttrModel('pkey', data={b'spam' * 1000})
    new_rcu = new_model.save()['ConsumedCapacity']['CapacityUnits']

    assert old_rcu == 6
    assert new_rcu == 4


@pytest.mark.ddblocal
def test_binary_set_attribute_update(ddb_url):
    class DataModel(Model):
        class Meta:
            table_name = 'binary_set_attr_update'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinaryDataSetAttribute()

    DataModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    data = {b'\x00hey\xfb', b'\x00beautiful\xfb'}
    pkey = 'pkey'
    DataModel(pkey, data=data).save()
    m = DataModel.get(pkey)
    assert m.data == data

    new_data = {b'\xff'}
    m.update(actions=[DataModel.data.set(new_data)])
    assert new_data == m.data

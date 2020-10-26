import pytest

from pynamodb.attributes import UnicodeAttribute, BinaryAttribute, BinarySetAttribute, CompressedAttribute
from pynamodb.models import Model


@pytest.mark.ddblocal
def test_binary_attribute_update(ddb_url):
    class DataModel(Model):
        class Meta:
            table_name = 'binary_attr_update'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinaryAttribute()

    DataModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    data = b'\x00hey\xfb'
    pkey = 'pkey'
    DataModel(pkey, data=data).save()
    m = DataModel.get(pkey)
    assert m.data == data

    new_data = b'\xff'
    m.update(actions=[DataModel.data.set(new_data)])
    assert new_data == m.data

@pytest.mark.ddblocal
def test_compressed_attribute_writes_large_items(ddb_url):
    class DataModel(Model):
        class Meta:
            table_name = 'compressed_attr_update'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = CompressedAttribute()

    DataModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    # create a 1MB+ string of bytes to compress and put in the DB
    data = bytes('1234567890' * 1024 * 1024, 'utf-8')
    pkey = 'pkey'
    DataModel(pkey, data=data).save()
    m = DataModel.get(pkey)
    assert m.data == data

@pytest.mark.ddblocal
def test_binary_set_attribute_update(ddb_url):
    class DataModel(Model):
        class Meta:
            table_name = 'binary_set_attr_update'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinarySetAttribute()

    DataModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    data = {b'\x00hey\xfb', b'\x00beautiful\xfb'}
    pkey = 'pkey'
    DataModel(pkey, data=data).save()
    m = DataModel.get(pkey)
    assert m.data == data

    new_data = {b'\xff'}
    m.update(actions=[DataModel.data.set(new_data)])
    assert new_data == m.data

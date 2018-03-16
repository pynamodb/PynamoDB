import pytest

<<<<<<< HEAD
from pynamodb.attributes import UnicodeAttribute, BinaryAttribute, BinarySetAttribute
=======
from pynamodb.attributes import UnicodeAttribute, BinaryAttribute
>>>>>>> ae03f5571249206eaf376791e5efb66645e0728b
from pynamodb.models import Model


@pytest.mark.ddblocal
<<<<<<< HEAD
def test_binary_attribute_update(ddb_url):
=======
def test_update(ddb_url):
>>>>>>> ae03f5571249206eaf376791e5efb66645e0728b
    class DataModel(Model):
        class Meta:
            table_name = 'binary_attr_update'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinaryAttribute()

    DataModel.create_table(read_capacity_units=1, write_capacity_units=1)
    data = b'\x00hey\xfb'
    pkey = 'pkey'
    DataModel(pkey, data=data).save()
    m = DataModel.get(pkey)
    assert m.data == data

    new_data = b'\xff'
    m.update(actions=[DataModel.data.set(new_data)])
    assert new_data == m.data
<<<<<<< HEAD

@pytest.mark.ddblocal
def test_binary_set_attribute_update(ddb_url):
    class DataModel(Model):
        class Meta:
            table_name = 'binary_set_attr_update'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinarySetAttribute()

    DataModel.create_table(read_capacity_units=1, write_capacity_units=1)
    data = [b'\x00hey\xfb', b'\x00beautiful\xfb']
    pkey = 'pkey'
    DataModel(pkey, data=data).save()
    m = DataModel.get(pkey)
    assert m.data == data

    new_data = b'\xff'
    m.update(actions=[DataModel.data.set(new_data)])
    assert new_data == m.data
=======
>>>>>>> ae03f5571249206eaf376791e5efb66645e0728b

import pytest

from pynamodb.attributes import UnicodeAttribute, BinaryAttribute, BinarySetAttribute
from pynamodb.models import Model


@pytest.mark.ddblocal
@pytest.mark.parametrize('legacy_encoding', [False, True])
def test_binary_set_attribute_update(legacy_encoding: bool, ddb_url: str) -> None:
    class DataModel(Model):
        class Meta:
            table_name = f'binary_attr_update__legacy_{legacy_encoding}'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinaryAttribute(legacy_encoding=legacy_encoding)

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
@pytest.mark.parametrize('legacy_encoding', [False, True])
def test_binary_set_attribute_update(legacy_encoding: bool, ddb_url: str) -> None:
    class DataModel(Model):
        class Meta:
            table_name = f'binary_set_attr_update__legacy_{legacy_encoding}'
            host = ddb_url
        pkey = UnicodeAttribute(hash_key=True)
        data = BinarySetAttribute(legacy_encoding=legacy_encoding)

    DataModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)
    data = {b'\x00hey\xfb', b'\x00beautiful\xfb'}
    pkey = 'pkey'
    DataModel(pkey, data=data).save()
    m = DataModel.get(pkey)
    assert m.data == data

    new_data = {b'\xff'}
    m.update(actions=[DataModel.data.set(new_data)])
    assert new_data == m.data

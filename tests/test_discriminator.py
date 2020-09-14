import pytest

from pynamodb.attributes import DiscriminatorAttribute
from pynamodb.attributes import ListAttribute
from pynamodb.attributes import MapAttribute
from pynamodb.attributes import NumberAttribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model


class_name = lambda cls: cls.__name__


class TypedValue(MapAttribute):
    _cls = DiscriminatorAttribute(attr_name = 'cls')
    name = UnicodeAttribute()


class NumberValue(TypedValue, discriminator=class_name):
    value = NumberAttribute()


class StringValue(TypedValue, discriminator=class_name):
    value = UnicodeAttribute()


class RenamedValue(TypedValue, discriminator='custom_name'):
    value = UnicodeAttribute()


class DiscriminatorTestModel(Model):
    class Meta:
        host = 'http://localhost:8000'
        table_name = 'test'
    hash_key = UnicodeAttribute(hash_key=True)
    value = TypedValue()
    values = ListAttribute(of=TypedValue)


class TestDiscriminatorAttribute:

    def test_serialize(self):
        dtm = DiscriminatorTestModel()
        dtm.hash_key = 'foo'
        dtm.value = StringValue(name='foo', value='Hello')
        dtm.values = [NumberValue(name='bar', value=5), RenamedValue(name='baz', value='World')]
        assert dtm._serialize() == {
            'HASH': 'foo',
            'attributes': {
                'value': {'M': {'cls': {'S': 'StringValue'}, 'name': {'S': 'foo'}, 'value': {'S': 'Hello'}}},
                'values': {'L': [
                    {'M': {'cls': {'S': 'NumberValue'}, 'name': {'S': 'bar'}, 'value': {'N': '5'}}},
                    {'M': {'cls': {'S': 'custom_name'}, 'name': {'S': 'baz'}, 'value': {'S': 'World'}}}
                ]}
            }
        }

    def test_deserialize(self):
        item = {
            'hash_key': {'S': 'foo'},
            'value': {'M': {'cls': {'S': 'StringValue'}, 'name': {'S': 'foo'}, 'value': {'S': 'Hello'}}},
            'values': {'L': [
                {'M': {'cls': {'S': 'NumberValue'}, 'name': {'S': 'bar'}, 'value': {'N': '5'}}},
                {'M': {'cls': {'S': 'custom_name'}, 'name': {'S': 'baz'}, 'value': {'S': 'World'}}}
            ]}
        }
        dtm = DiscriminatorTestModel.from_raw_data(item)
        assert dtm.hash_key == 'foo'
        assert dtm.value.value == 'Hello'
        assert dtm.values[0].value == 5
        assert dtm.values[1].value == 'World'

    def test_condition_expression(self):
        condition = DiscriminatorTestModel.value._cls == RenamedValue
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0.#1 = :0"
        assert placeholder_names == {'value': '#0', 'cls': '#1'}
        assert expression_attribute_values == {':0': {'S': 'custom_name'}}

    def test_multiple_discriminator_values(self):
        class TestAttribute(MapAttribute, discriminator='new_value'):
            cls = DiscriminatorAttribute()

        TestAttribute.cls.register_class(TestAttribute, 'old_value')

        # ensure the first registered value is used during serialization
        assert TestAttribute.cls.get_discriminator(TestAttribute) == 'new_value'
        assert TestAttribute.cls.serialize(TestAttribute) == 'new_value'

        # ensure the second registered value can be used to deserialize
        assert TestAttribute.cls.deserialize('old_value') == TestAttribute
        assert TestAttribute.cls.deserialize('new_value') == TestAttribute

    def test_multiple_discriminator_classes(self):
        with pytest.raises(ValueError):
            # fail when attempting to register a class with an existing discriminator value
            class RenamedValue2(TypedValue, discriminator='custom_name'):
                pass

    def test_model(self):
        with pytest.raises(NotImplementedError):
            class DiscriminatedModel(Model):
                hash_key = UnicodeAttribute(hash_key=True)
                _cls = DiscriminatorAttribute()

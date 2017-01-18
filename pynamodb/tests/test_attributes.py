"""
pynamodb attributes tests
"""
import six
import json
from base64 import b64encode
from datetime import datetime
from delorean import Delorean
from mock import patch
from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.constants import UTC, DATETIME_FORMAT
from pynamodb.models import Model
from pynamodb.attributes import (
    BinarySetAttribute, BinaryAttribute, NumberSetAttribute, NumberAttribute,
    UnicodeAttribute, UnicodeSetAttribute, UTCDateTimeAttribute, BooleanAttribute, LegacyBooleanAttribute,
    MapAttribute, ListAttribute,
    JSONAttribute, DEFAULT_ENCODING, NUMBER, STRING, STRING_SET, NUMBER_SET, BINARY_SET,
    BINARY, MAP, LIST, BOOLEAN)


class AttributeTestModel(Model):

    class Meta:
        host = 'http://localhost:8000'
        table_name = 'test'

    binary_attr = BinaryAttribute()
    binary_set_attr = BinarySetAttribute()
    number_attr = NumberAttribute()
    number_set_attr = NumberSetAttribute()
    unicode_attr = UnicodeAttribute()
    unicode_set_attr = UnicodeSetAttribute()
    datetime_attr = UTCDateTimeAttribute()
    bool_attr = BooleanAttribute()
    json_attr = JSONAttribute()
    map_attr = MapAttribute()


class CustomAttrMap(MapAttribute):
    overridden_number_attr = NumberAttribute(attr_name="number_attr")
    overridden_unicode_attr = UnicodeAttribute(attr_name="unicode_attr")


class DefaultsMap(MapAttribute):
    map_field = MapAttribute(default={})


class AttributeDescriptorTestCase(TestCase):
    """
    Test Attribute Descriptors
    """
    def setUp(self):
        self.instance = AttributeTestModel()

    def test_binary_attr(self):
        """
        Binary attribute descriptor
        """
        self.instance.binary_attr = b'test'
        self.assertEqual(self.instance.binary_attr, b'test')

    def test_binary_set_attr(self):
        """
        Binary set attribute descriptor
        """
        self.instance.binary_set_attr = set([b'test', b'test2'])
        self.assertEqual(self.instance.binary_set_attr, set([b'test', b'test2']))

    def test_number_attr(self):
        """
        Number attribute descriptor
        """
        self.instance.number_attr = 42
        self.assertEqual(self.instance.number_attr, 42)

    def test_number_set_attr(self):
        """
        Number set attribute descriptor
        """
        self.instance.number_set_attr = set([1, 2])
        self.assertEqual(self.instance.number_set_attr, set([1, 2]))

    def test_unicode_attr(self):
        """
        Unicode attribute descriptor
        """
        self.instance.unicode_attr = u"test"
        self.assertEqual(self.instance.unicode_attr, u"test")

    def test_unicode_set_attr(self):
        """
        Unicode set attribute descriptor
        """
        self.instance.unicode_set_attr = set([u"test", u"test2"])
        self.assertEqual(self.instance.unicode_set_attr, set([u"test", u"test2"]))

    def test_datetime_attr(self):
        """
        Datetime attribute descriptor
        """
        now = datetime.now()
        self.instance.datetime_attr = now
        self.assertEqual(self.instance.datetime_attr, now)

    def test_bool_attr(self):
        """
        Boolean attribute descriptor
        """
        self.instance.bool_attr = True
        self.assertEqual(self.instance.bool_attr, True)

    def test_json_attr(self):
        """
        JSON attribute descriptor
        """
        self.instance.json_attr = {'foo': 'bar', 'bar': 42}
        self.assertEqual(self.instance.json_attr, {'foo': 'bar', 'bar': 42})


class UTCDateTimeAttributeTestCase(TestCase):
    """
    Tests UTCDateTime attributes
    """
    def test_utc_datetime_attribute(self):
        """
        UTCDateTimeAttribute.default
        """
        attr = UTCDateTimeAttribute()
        self.assertIsNotNone(attr)
        self.assertEqual(attr.attr_type, STRING)
        tstamp = datetime.now()
        attr = UTCDateTimeAttribute(default=tstamp)
        self.assertEqual(attr.default, tstamp)

    def test_utc_date_time_deserialize(self):
        """
        UTCDateTimeAttribute.deserialize
        """
        tstamp = Delorean(timezone=UTC).datetime
        attr = UTCDateTimeAttribute()
        self.assertEqual(
            tstamp,
            attr.deserialize(Delorean(tstamp, timezone=UTC).datetime.strftime(DATETIME_FORMAT)),
        )

    def test_utc_date_time_deserialize_parse_args(self):
        """
        UTCDateTimeAttribute.deserialize
        """
        tstamp = Delorean(timezone=UTC).datetime
        attr = UTCDateTimeAttribute()

        with patch('pynamodb.attributes.parse') as parse:
            attr.deserialize(Delorean(tstamp, timezone=UTC).datetime.strftime(DATETIME_FORMAT))

            parse.assert_called_with(tstamp.strftime(DATETIME_FORMAT), dayfirst=False)

    def test_utc_date_time_serialize(self):
        """
        UTCDateTimeAttribute.serialize
        """
        tstamp = datetime.now()
        attr = UTCDateTimeAttribute()
        self.assertEqual(attr.serialize(tstamp), Delorean(tstamp, timezone=UTC).datetime.strftime(DATETIME_FORMAT))


class BinaryAttributeTestCase(TestCase):
    """
    Tests binary attributes
    """
    def test_binary_attribute(self):
        """
        BinaryAttribute.default
        """
        attr = BinaryAttribute()
        self.assertIsNotNone(attr)
        self.assertEqual(attr.attr_type, BINARY)

        attr = BinaryAttribute(default=b'foo')
        self.assertEqual(attr.default, b'foo')

    def test_binary_round_trip(self):
        """
        BinaryAttribute round trip
        """
        attr = BinaryAttribute()
        value = b'foo'
        serial = attr.serialize(value)
        self.assertEqual(attr.deserialize(serial), value)

    def test_binary_serialize(self):
        """
        BinaryAttribute.serialize
        """
        attr = BinaryAttribute()
        serial = b64encode(b'foo').decode(DEFAULT_ENCODING)
        self.assertEqual(attr.serialize(b'foo'), serial)

    def test_binary_deserialize(self):
        """
        BinaryAttribute.deserialize
        """
        attr = BinaryAttribute()
        serial = b64encode(b'foo').decode(DEFAULT_ENCODING)
        self.assertEqual(attr.deserialize(serial), b'foo')

    def test_binary_set_serialize(self):
        """
        BinarySetAttribute.serialize
        """
        attr = BinarySetAttribute()
        self.assertEqual(attr.attr_type, BINARY_SET)
        self.assertEqual(
            attr.serialize(set([b'foo', b'bar'])),
            [b64encode(val).decode(DEFAULT_ENCODING) for val in sorted(set([b'foo', b'bar']))])
        self.assertEqual(attr.serialize(None), None)

    def test_binary_set_round_trip(self):
        """
        BinarySetAttribute round trip
        """
        attr = BinarySetAttribute()
        value = set([b'foo', b'bar'])
        serial = attr.serialize(value)
        self.assertEqual(attr.deserialize(serial), value)

    def test_binary_set_deserialize(self):
        """
        BinarySetAttribute.deserialize
        """
        attr = BinarySetAttribute()
        value = set([b'foo', b'bar'])
        self.assertEqual(
            attr.deserialize([b64encode(val).decode(DEFAULT_ENCODING) for val in sorted(value)]),
            value
        )

    def test_binary_set_attribute(self):
        """
        BinarySetAttribute.serialize
        """
        attr = BinarySetAttribute()
        self.assertIsNotNone(attr)

        attr = BinarySetAttribute(default=set([b'foo', b'bar']))
        self.assertEqual(attr.default, set([b'foo', b'bar']))


class NumberAttributeTestCase(TestCase):
    """
    Tests number attributes
    """
    def test_number_attribute(self):
        """
        NumberAttribute.default
        """
        attr = NumberAttribute()
        self.assertIsNotNone(attr)
        self.assertEqual(attr.attr_type, NUMBER)

        attr = NumberAttribute(default=1)
        self.assertEqual(attr.default, 1)

    def test_number_serialize(self):
        """
        NumberAttribute.serialize
        """
        attr = NumberAttribute()
        self.assertEqual(attr.serialize(3.141), '3.141')
        self.assertEqual(attr.serialize(1), '1')
        self.assertEqual(attr.serialize(12345678909876543211234234324234), '12345678909876543211234234324234')

    def test_number_deserialize(self):
        """
        NumberAttribute.deserialize
        """
        attr = NumberAttribute()
        self.assertEqual(attr.deserialize('1'), 1)
        self.assertEqual(attr.deserialize('3.141'), 3.141)
        self.assertEqual(attr.deserialize('12345678909876543211234234324234'), 12345678909876543211234234324234)

    def test_number_set_deserialize(self):
        """
        NumberSetAttribute.deserialize
        """
        attr = NumberSetAttribute()
        self.assertEqual(attr.attr_type, NUMBER_SET)
        self.assertEqual(attr.deserialize([json.dumps(val) for val in sorted(set([1, 2]))]), set([1, 2]))

    def test_number_set_serialize(self):
        """
        NumberSetAttribute.serialize
        """
        attr = NumberSetAttribute()
        self.assertEqual(attr.serialize(set([1, 2])), [json.dumps(val) for val in sorted(set([1, 2]))])
        self.assertEqual(attr.serialize(None), None)

    def test_number_set_attribute(self):
        """
        NumberSetAttribute.default
        """
        attr = NumberSetAttribute()
        self.assertIsNotNone(attr)

        attr = NumberSetAttribute(default=set([1, 2]))
        self.assertEqual(attr.default, set([1, 2]))


class UnicodeAttributeTestCase(TestCase):
    """
    Tests unicode attributes
    """
    def test_unicode_attribute(self):
        """
        UnicodeAttribute.default
        """
        attr = UnicodeAttribute()
        self.assertIsNotNone(attr)
        self.assertEqual(attr.attr_type, STRING)

        attr = UnicodeAttribute(default=six.u('foo'))
        self.assertEqual(attr.default, six.u('foo'))

    def test_unicode_serialize(self):
        """
        UnicodeAttribute.serialize
        """
        attr = UnicodeAttribute()
        self.assertEqual(attr.serialize('foo'), six.u('foo'))
        self.assertEqual(attr.serialize(u'foo'), six.u('foo'))
        self.assertEqual(attr.serialize(u''), None)
        self.assertEqual(attr.serialize(None), None)

    def test_unicode_deserialize(self):
        """
        UnicodeAttribute.deserialize
        """
        attr = UnicodeAttribute()
        self.assertEqual(attr.deserialize('foo'), six.u('foo'))
        self.assertEqual(attr.deserialize(u'foo'), six.u('foo'))

    def test_unicode_set_serialize(self):
        """
        UnicodeSetAttribute.serialize
        """
        attr = UnicodeSetAttribute()
        self.assertEqual(attr.attr_type, STRING_SET)
        self.assertEqual(attr.deserialize(None), None)
        self.assertEqual(
            attr.serialize(set([six.u('foo'), six.u('bar')])),
            sorted([six.u('foo'), six.u('bar')])
        )

    def test_round_trip_unicode_set(self):
        """
        Round trip a unicode set
        """
        attr = UnicodeSetAttribute()
        orig = set([six.u('foo'), six.u('bar')])
        self.assertEqual(
            orig,
            attr.deserialize(attr.serialize(orig))
        )

    def test_unicode_set_deserialize(self):
        """
        UnicodeSetAttribute.deserialize
        """
        attr = UnicodeSetAttribute()
        value = set([six.u('foo'), six.u('bar')])
        self.assertEqual(
            attr.deserialize(value),
            value
        )

    def test_unicode_set_deserialize(self):
        """
        UnicodeSetAttribute.deserialize old way
        """
        attr = UnicodeSetAttribute()
        value = set([six.u('foo'), six.u('bar')])
        old_value = set([json.dumps(val) for val in value])
        self.assertEqual(
            attr.deserialize(old_value),
            value
        )

    def test_unicode_set_attribute(self):
        """
        UnicodeSetAttribute.default
        """
        attr = UnicodeSetAttribute()
        self.assertIsNotNone(attr)
        self.assertEqual(attr.attr_type, STRING_SET)
        attr = UnicodeSetAttribute(default=set([six.u('foo'), six.u('bar')]))
        self.assertEqual(attr.default, set([six.u('foo'), six.u('bar')]))


class LegacyBooleanAttributeTestCase(TestCase):
    def test_legacy_boolean_attribute_can_read_future_boolean_attributes(self):
        """
        LegacyBooleanAttribute.deserialize
        :return:
        """
        attr = LegacyBooleanAttribute()
        self.assertEqual(attr.deserialize('1'), True)
        self.assertEqual(attr.deserialize('0'), False)
        self.assertEqual(attr.deserialize(json.dumps(True)), True)
        self.assertEqual(attr.deserialize(json.dumps(False)), False)

    def test_legacy_boolean_attribute_get_value_can_read_both(self):
        """
        LegacyBooleanAttribute.get_value
        :return:
        """
        attr = LegacyBooleanAttribute()
        self.assertEqual(attr.get_value({'N': '1'}), '1')
        self.assertEqual(attr.get_value({'N': '0'}), '0')
        self.assertEqual(attr.get_value({'BOOL': True}), json.dumps(True))
        self.assertEqual(attr.get_value({'BOOL': False}), json.dumps(False))

    def test_legacy_boolean_attribute_get_value_and_deserialize_work_together(self):
        attr = LegacyBooleanAttribute()
        self.assertEqual(attr.deserialize(attr.get_value({'N': '1'})), True)
        self.assertEqual(attr.deserialize(attr.get_value({'N': '0'})), False)
        self.assertEqual(attr.deserialize(attr.get_value({'BOOL': True})), True)
        self.assertEqual(attr.deserialize(attr.get_value({'BOOL': False})), False)

    def test_legacy_boolean_attribute_serialize(self):
        """
        LegacyBooleanAttribute.serialize
        """
        attr = LegacyBooleanAttribute()
        self.assertEqual(attr.serialize(True), '1')
        self.assertEqual(attr.serialize(False), '0')
        self.assertEqual(attr.serialize(None), None)


class BooleanAttributeTestCase(TestCase):
    """
    Tests boolean attributes
    """
    def test_boolean_attribute(self):
        """
        BooleanAttribute.default
        """
        attr = BooleanAttribute()
        self.assertIsNotNone(attr)

        self.assertEqual(attr.attr_type, BOOLEAN)
        attr = BooleanAttribute(default=True)
        self.assertEqual(attr.default, True)

    def test_boolean_serialize(self):
        """
        BooleanAttribute.serialize
        """
        attr = BooleanAttribute()
        self.assertEqual(attr.serialize(True), True)
        self.assertEqual(attr.serialize(False), False)
        self.assertEqual(attr.serialize(None), None)

    def test_boolean_deserialize(self):
        """
        BooleanAttribute.deserialize
        """
        attr = BooleanAttribute()
        self.assertEqual(attr.deserialize('1'), True)
        self.assertEqual(attr.deserialize('0'), True)
        self.assertEqual(attr.deserialize(True), True)
        self.assertEqual(attr.deserialize(False), False)


class JSONAttributeTestCase(TestCase):
    """
    Tests json attributes
    """
    def test_quoted_json(self):
        attr = JSONAttribute()
        serialized = attr.serialize('\\t')
        self.assertEqual(attr.deserialize(serialized), '\\t')

        serialized = attr.serialize('"')
        self.assertEqual(attr.deserialize(serialized), '"')

    def test_json_attribute(self):
        """
        JSONAttribute.default
        """
        attr = JSONAttribute()
        self.assertIsNotNone(attr)

        self.assertEqual(attr.attr_type, STRING)
        attr = JSONAttribute(default={})
        self.assertEqual(attr.default, {})

    def test_json_serialize(self):
        """
        JSONAttribute.serialize
        """
        attr = JSONAttribute()
        item = {'foo': 'bar', 'bool': True, 'number': 3.141}
        self.assertEqual(attr.serialize(item), six.u(json.dumps(item)))
        self.assertEqual(attr.serialize({}), six.u('{}'))
        self.assertEqual(attr.serialize(None), None)

    def test_json_deserialize(self):
        """
        JSONAttribute.deserialize
        """
        attr = JSONAttribute()
        item = {'foo': 'bar', 'bool': True, 'number': 3.141}
        encoded = six.u(json.dumps(item))
        self.assertEqual(attr.deserialize(encoded), item)

    def test_control_chars(self):
        """
        JSONAttribute with control chars
        """
        attr = JSONAttribute()
        item = {'foo\t': 'bar\n', 'bool': True, 'number': 3.141}
        encoded = six.u(json.dumps(item))
        self.assertEqual(attr.deserialize(encoded), item)


class MapAttributeTestCase(TestCase):
    """
    Tests map with str, int, float
    """
    def test_attribute_children(self):
        person_attribute = {
            'name': 'Justin',
            'age': 12345678909876543211234234324234,
            'height': 187.96
        }
        attr = MapAttribute()
        serialized = attr.serialize(person_attribute)
        self.assertEqual(attr.deserialize(serialized), person_attribute)

    def test_map_of_map(self):
        attribute = {
            'name': 'Justin',
            'metrics': {
                'age': 31,
                'height': 187.96
            }
        }
        attr = MapAttribute()
        serialized = attr.serialize(attribute)
        self.assertEqual(attr.deserialize(serialized), attribute)

    def test_map_overridden_attrs_accessors(self):
        attr = CustomAttrMap(**{
            'overridden_number_attr': 10,
            'overridden_unicode_attr': "Hello"
        })

        self.assertEqual(10, attr.overridden_number_attr)
        self.assertEqual("Hello", attr.overridden_unicode_attr)

    def test_map_overridden_attrs_serialize(self):
        attribute = {
            'overridden_number_attr': 10,
            'overridden_unicode_attr': "Hello"
        }
        self.assertEqual(
            {'number_attr': {'N': '10'}, 'unicode_attr': {'S': six.u('Hello')}},
            CustomAttrMap().serialize(attribute)
        )

    def test_defaults(self):
        item = DefaultsMap()
        self.assertTrue(item.validate())
        self.assertEquals(DefaultsMap().serialize(item), {
            'map_field': {
                'M': {}
            }
        })

    def test_raw_map_from_dict(self):
        item = AttributeTestModel(
            map_attr={
                "foo": "bar",
                "num": 3,
                "nested": {
                    "nestedfoo": "nestedbar"
                }
            }
        )

        self.assertEqual(item.map_attr['foo'], 'bar')
        self.assertEqual(item.map_attr['num'], 3)

    def test_raw_map_access(self):
        raw = {
            "foo": "bar",
            "num": 3,
            "nested": {
                "nestedfoo": "nestedbar"
            }
        }
        attr = MapAttribute(**raw)

        for k, v in six.iteritems(raw):
            self.assertEquals(attr[k], v)

    def test_raw_map_json_serialize(self):
        raw = {
            "foo": "bar",
            "num": 3,
            "nested": {
                "nestedfoo": "nestedbar"
            }
        }

        serialized_raw = json.dumps(raw)
        self.assertEqual(json.dumps(AttributeTestModel(map_attr=raw).map_attr.as_dict()),
                         serialized_raw)
        self.assertEqual(json.dumps(AttributeTestModel(map_attr=MapAttribute(**raw)).map_attr.as_dict()),
                         serialized_raw)

    def test_typed_and_raw_map_json_serialize(self):
        class TypedMap(MapAttribute):
            map_attr = MapAttribute()

        class SomeModel(Model):
            typed_map = TypedMap()

        item = SomeModel(
            typed_map=TypedMap(map_attr={'foo': 'bar'})
        )

        self.assertEqual(json.dumps({'map_attr': {'foo': 'bar'}}),
                         json.dumps(item.typed_map.as_dict()))


class MapAndListAttributeTestCase(TestCase):

    def test_map_of_list(self):
        grocery_list = {
            'fruit': ['apple', 'pear', 32],
            'veggies': ['broccoli', 'potatoes', 5]
        }
        serialized = MapAttribute().serialize(grocery_list)
        self.assertEqual(MapAttribute().deserialize(serialized), grocery_list)

    def test_map_of_list_of_map(self):
        family_attributes = {
            'phillips': [
                {
                    'name': 'Justin',
                    'age': 31,
                    'height': 187.96
                },
                {
                    'name': 'Galen',
                    'age': 29,
                    'height': 193.04,
                    'male': True
                },
                {
                    'name': 'Colin',
                    'age': 32,
                    'height': 203.2,
                    'male': True,
                    'hasChild': True
                }
            ],
            'workman': [
                {
                    'name': 'Mandy',
                    'age': 29,
                    'height': 157.48,
                    'female': True
                },
                {
                    'name': 'Rodney',
                    'age': 31,
                    'height': 175.26,
                    'hasChild': False
                }
            ]
        }
        serialized = MapAttribute().serialize(family_attributes)
        self.assertDictEqual(MapAttribute().deserialize(serialized), family_attributes)

    def test_list_of_map_with_of(self):
        class Person(MapAttribute):
            name = UnicodeAttribute()
            age = NumberAttribute()

            def __lt__(self, other):
                return self.name < other.name

            def __eq__(self, other):
                return self.name == other.name and \
                       self.age == other.age

        person1 = Person()
        person1.name = 'john'
        person1.age = 40

        person2 = Person()
        person2.name = 'Dana'
        person2.age = 41
        inp = [person1, person2]

        list_attribute = ListAttribute(default=[], of=Person)
        serialized = list_attribute.serialize(inp)
        deserialized = list_attribute.deserialize(serialized)
        self.assertEqual(sorted(deserialized), sorted(inp))

    def test_list_of_unicode_with_of(self):
        with self.assertRaises(ValueError):
            ListAttribute(default=[], of=UnicodeAttribute)

    def test_list_of_unicode(self):
        list_attribute = ListAttribute()
        inp = ['rome', 'berlin']
        serialized = list_attribute.serialize(inp)
        deserialized = list_attribute.deserialize(serialized)
        self.assertEqual(sorted(deserialized), sorted(inp))

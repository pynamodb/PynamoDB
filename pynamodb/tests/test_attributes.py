"""
pynamodb attributes tests
"""
import json
import six

from base64 import b64encode
from datetime import datetime

from dateutil.parser import parse
from dateutil.tz import tzutc

from mock import patch, Mock, call
import pytest

from pynamodb.attributes import (
    BinarySetAttribute, BinaryAttribute, NumberSetAttribute, NumberAttribute,
    UnicodeAttribute, UnicodeSetAttribute, UTCDateTimeAttribute, BooleanAttribute, LegacyBooleanAttribute,
    MapAttribute, MapAttributeMeta, ListAttribute, JSONAttribute, _get_value_for_deserialize,
)
from pynamodb.constants import (
    DATETIME_FORMAT, DEFAULT_ENCODING, NUMBER, STRING, STRING_SET, NUMBER_SET, BINARY_SET,
    BINARY, BOOLEAN,
)
from pynamodb.models import Model


UTC = tzutc()


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
    string_field = UnicodeAttribute(null=True)


class TestAttributeDescriptor:
    """
    Test Attribute Descriptors
    """
    def setup(self):
        self.instance = AttributeTestModel()

    def test_binary_attr(self):
        """
        Binary attribute descriptor
        """
        self.instance.binary_attr = b'test'
        assert self.instance.binary_attr == b'test'

    def test_binary_set_attr(self):
        """
        Binary set attribute descriptor
        """
        self.instance.binary_set_attr = set([b'test', b'test2'])
        assert self.instance.binary_set_attr == set([b'test', b'test2'])

    def test_number_attr(self):
        """
        Number attribute descriptor
        """
        self.instance.number_attr = 42
        assert self.instance.number_attr == 42

    def test_number_set_attr(self):
        """
        Number set attribute descriptor
        """
        self.instance.number_set_attr = set([1, 2])
        assert self.instance.number_set_attr == set([1, 2])

    def test_unicode_attr(self):
        """
        Unicode attribute descriptor
        """
        self.instance.unicode_attr = u"test"
        assert self.instance.unicode_attr == u"test"

    def test_unicode_set_attr(self):
        """
        Unicode set attribute descriptor
        """
        self.instance.unicode_set_attr = set([u"test", u"test2"])
        assert self.instance.unicode_set_attr == set([u"test", u"test2"])

    def test_datetime_attr(self):
        """
        Datetime attribute descriptor
        """
        now = datetime.now()
        self.instance.datetime_attr = now
        assert self.instance.datetime_attr == now

    def test_bool_attr(self):
        """
        Boolean attribute descriptor
        """
        self.instance.bool_attr = True
        assert self.instance.bool_attr is True

    def test_json_attr(self):
        """
        JSON attribute descriptor
        """
        self.instance.json_attr = {'foo': 'bar', 'bar': 42}
        assert self.instance.json_attr == {'foo': 'bar', 'bar': 42}


class TestUTCDateTimeAttribute:
    """
    Tests UTCDateTime attributes
    """
    def test_utc_datetime_attribute(self):
        """
        UTCDateTimeAttribute.default
        """
        attr = UTCDateTimeAttribute()
        assert attr is not None
        assert attr.attr_type == STRING
        tstamp = datetime.now()
        attr = UTCDateTimeAttribute(default=tstamp)
        assert attr.default == tstamp

    def test_utc_date_time_deserialize(self):
        """
        UTCDateTimeAttribute.deserialize
        """
        tstamp = datetime.now(UTC)
        attr = UTCDateTimeAttribute()
        assert attr.deserialize(tstamp.strftime(DATETIME_FORMAT)) == tstamp

    def test_dateutil_parser_fallback(self):
        """
        UTCDateTimeAttribute.deserialize
        """
        expected_value = datetime(2047, 1, 6, 8, 21, tzinfo=tzutc())
        attr = UTCDateTimeAttribute()
        assert attr.deserialize('January 6, 2047 at 8:21:00AM UTC') == expected_value

    @patch('pynamodb.attributes.datetime')
    @patch('pynamodb.attributes.parse')
    def test_utc_date_time_deserialize_parse_args(self, parse_mock, datetime_mock):
        """
        UTCDateTimeAttribute.deserialize
        """
        tstamp = datetime.now(UTC)
        attr = UTCDateTimeAttribute()

        tstamp_str = tstamp.strftime(DATETIME_FORMAT)
        attr.deserialize(tstamp_str)

        parse_mock.assert_not_called()
        datetime_mock.strptime.assert_called_once_with(tstamp_str, DATETIME_FORMAT)

    def test_utc_date_time_serialize(self):
        """
        UTCDateTimeAttribute.serialize
        """
        tstamp = datetime.now()
        attr = UTCDateTimeAttribute()
        assert attr.serialize(tstamp) == tstamp.replace(tzinfo=UTC).strftime(DATETIME_FORMAT)


class TestBinaryAttribute:
    """
    Tests binary attributes
    """
    def test_binary_attribute(self):
        """
        BinaryAttribute.default
        """
        attr = BinaryAttribute()
        assert attr is not None
        assert attr.attr_type == BINARY

        attr = BinaryAttribute(default=b'foo')
        assert attr.default == b'foo'

    def test_binary_round_trip(self):
        """
        BinaryAttribute round trip
        """
        attr = BinaryAttribute()
        value = b'foo'
        serial = attr.serialize(value)
        assert attr.deserialize(serial) == value

    def test_binary_serialize(self):
        """
        BinaryAttribute.serialize
        """
        attr = BinaryAttribute()
        serial = b64encode(b'foo').decode(DEFAULT_ENCODING)
        assert attr.serialize(b'foo') == serial

    def test_binary_deserialize(self):
        """
        BinaryAttribute.deserialize
        """
        attr = BinaryAttribute()
        serial = b64encode(b'foo').decode(DEFAULT_ENCODING)
        assert attr.deserialize(serial) == b'foo'

    def test_binary_set_serialize(self):
        """
        BinarySetAttribute.serialize
        """
        attr = BinarySetAttribute()
        assert attr.attr_type == BINARY_SET
        assert attr.serialize(set([b'foo', b'bar'])) == [
            b64encode(val).decode(DEFAULT_ENCODING) for val in sorted(set([b'foo', b'bar']))
        ]
        assert attr.serialize(None) is None

    def test_binary_set_round_trip(self):
        """
        BinarySetAttribute round trip
        """
        attr = BinarySetAttribute()
        value = set([b'foo', b'bar'])
        serial = attr.serialize(value)
        assert attr.deserialize(serial) == value

    def test_binary_set_deserialize(self):
        """
        BinarySetAttribute.deserialize
        """
        attr = BinarySetAttribute()
        value = set([b'foo', b'bar'])
        assert attr.deserialize(
            [b64encode(val).decode(DEFAULT_ENCODING) for val in sorted(value)]
        ) == value

    def test_binary_set_attribute(self):
        """
        BinarySetAttribute.serialize
        """
        attr = BinarySetAttribute()
        assert attr is not None

        attr = BinarySetAttribute(default=set([b'foo', b'bar']))
        assert attr.default == set([b'foo', b'bar'])


class TestNumberAttribute:
    """
    Tests number attributes
    """
    def test_number_attribute(self):
        """
        NumberAttribute.default
        """
        attr = NumberAttribute()
        assert attr is not None
        assert attr.attr_type == NUMBER

        attr = NumberAttribute(default=1)
        assert attr.default == 1

    def test_number_serialize(self):
        """
        NumberAttribute.serialize
        """
        attr = NumberAttribute()
        assert attr.serialize(3.141) == '3.141'
        assert attr.serialize(1) == '1'
        assert attr.serialize(12345678909876543211234234324234) == '12345678909876543211234234324234'

    def test_number_deserialize(self):
        """
        NumberAttribute.deserialize
        """
        attr = NumberAttribute()
        assert attr.deserialize('1') == 1
        assert attr.deserialize('3.141') == 3.141
        assert attr.deserialize('12345678909876543211234234324234') == 12345678909876543211234234324234

    def test_number_set_deserialize(self):
        """
        NumberSetAttribute.deserialize
        """
        attr = NumberSetAttribute()
        assert attr.attr_type == NUMBER_SET
        assert attr.deserialize([json.dumps(val) for val in sorted(set([1, 2]))]) == set([1, 2])

    def test_number_set_serialize(self):
        """
        NumberSetAttribute.serialize
        """
        attr = NumberSetAttribute()
        assert attr.serialize(set([1, 2])) == [json.dumps(val) for val in sorted(set([1, 2]))]
        assert attr.serialize(None) is None

    def test_number_set_attribute(self):
        """
        NumberSetAttribute.default
        """
        attr = NumberSetAttribute()
        assert attr is not None

        attr = NumberSetAttribute(default=set([1, 2]))
        assert attr.default == set([1, 2])


class TestUnicodeAttribute:
    """
    Tests unicode attributes
    """
    def test_unicode_attribute(self):
        """
        UnicodeAttribute.default
        """
        attr = UnicodeAttribute()
        assert attr is not None
        assert attr.attr_type == STRING

        attr = UnicodeAttribute(default=six.u('foo'))
        assert attr.default == six.u('foo')

    def test_unicode_serialize(self):
        """
        UnicodeAttribute.serialize
        """
        attr = UnicodeAttribute()
        assert attr.serialize('foo') == six.u('foo')
        assert attr.serialize(u'foo') == six.u('foo')
        assert attr.serialize(u'') is None
        assert attr.serialize(None) is None

    def test_unicode_deserialize(self):
        """
        UnicodeAttribute.deserialize
        """
        attr = UnicodeAttribute()
        assert attr.deserialize('foo') == six.u('foo')
        assert attr.deserialize(u'foo') == six.u('foo')

    def test_unicode_set_serialize(self):
        """
        UnicodeSetAttribute.serialize
        """
        attr = UnicodeSetAttribute()
        assert attr.attr_type == STRING_SET
        assert attr.deserialize(None) is None

        expected = sorted([six.u('foo'), six.u('bar')])
        assert attr.serialize(set([six.u('foo'), six.u('bar')])) == expected

        expected = sorted([six.u('True'), six.u('False')])
        assert attr.serialize(set([six.u('True'), six.u('False')])) == expected

        expected = sorted([six.u('true'), six.u('false')])
        assert attr.serialize(set([six.u('true'), six.u('false')])) == expected

    def test_round_trip_unicode_set(self):
        """
        Round trip a unicode set
        """
        attr = UnicodeSetAttribute()
        orig = set([six.u('foo'), six.u('bar')])
        assert orig == attr.deserialize(attr.serialize(orig))

        orig = set([six.u('true'), six.u('false')])
        assert orig == attr.deserialize(attr.serialize(orig))

        orig = set([six.u('1'), six.u('2.8')])
        assert orig == attr.deserialize(attr.serialize(orig))

        orig = set([six.u('[1,2,3]'), six.u('2.8')])
        assert orig == attr.deserialize(attr.serialize(orig))

    def test_unicode_set_deserialize(self):
        """
        UnicodeSetAttribute.deserialize
        """
        attr = UnicodeSetAttribute()
        value = set([six.u('foo'), six.u('bar')])
        assert attr.deserialize(value) == value

        value = set([six.u('True'), six.u('False')])
        assert attr.deserialize(value) == value

        value = set([six.u('true'), six.u('false')])
        assert attr.deserialize(value) == value

        value = set([six.u('1'), six.u('2.8')])
        assert attr.deserialize(value) == value

    def test_unicode_set_attribute(self):
        """
        UnicodeSetAttribute.default
        """
        attr = UnicodeSetAttribute()
        assert attr is not None
        assert attr.attr_type == STRING_SET
        attr = UnicodeSetAttribute(default=set([six.u('foo'), six.u('bar')]))
        assert attr.default == set([six.u('foo'), six.u('bar')])


class TestLegacyBooleanAttribute:
    def test_legacy_boolean_attribute_can_read_future_boolean_attributes(self):
        """
        LegacyBooleanAttribute.deserialize
        :return:
        """
        attr = LegacyBooleanAttribute()
        assert attr.deserialize('1') is True
        assert attr.deserialize('0') is False
        assert attr.deserialize(json.dumps(True)) is True
        assert attr.deserialize(json.dumps(False)) is False

    def test_legacy_boolean_attribute_get_value_can_read_both(self):
        """
        LegacyBooleanAttribute.get_value
        :return:
        """
        attr = LegacyBooleanAttribute()
        assert attr.get_value({'N': '1'}) == '1'
        assert attr.get_value({'N': '0'}) == '0'
        assert attr.get_value({'BOOL': True}) == json.dumps(True)
        assert attr.get_value({'BOOL': False}) == json.dumps(False)

    def test_legacy_boolean_attribute_get_value_and_deserialize_work_together(self):
        attr = LegacyBooleanAttribute()
        assert attr.deserialize(attr.get_value({'N': '1'})) is True
        assert attr.deserialize(attr.get_value({'N': '0'})) is False
        assert attr.deserialize(attr.get_value({'BOOL': True})) is True
        assert attr.deserialize(attr.get_value({'BOOL': False})) is False

    def test_legacy_boolean_attribute_serialize(self):
        """
        LegacyBooleanAttribute.serialize
        """
        attr = LegacyBooleanAttribute()
        assert attr.serialize(True) == '1'
        assert attr.serialize(False) == '0'
        assert attr.serialize(None) is None


class TestBooleanAttribute:
    """
    Tests boolean attributes
    """
    def test_boolean_attribute(self):
        """
        BooleanAttribute.default
        """
        attr = BooleanAttribute()
        assert attr is not None

        assert attr.attr_type == BOOLEAN
        attr = BooleanAttribute(default=True)
        assert attr.default is True

    def test_boolean_serialize(self):
        """
        BooleanAttribute.serialize
        """
        attr = BooleanAttribute()
        assert attr.serialize(True) is True
        assert attr.serialize(False) is False
        assert attr.serialize(None) is None

    def test_boolean_deserialize(self):
        """
        BooleanAttribute.deserialize
        """
        attr = BooleanAttribute()
        assert attr.deserialize('1') is True
        assert attr.deserialize('0') is True
        assert attr.deserialize(True) is True
        assert attr.deserialize(False) is False


class TestJSONAttribute:
    """
    Tests json attributes
    """
    def test_quoted_json(self):
        attr = JSONAttribute()
        serialized = attr.serialize('\\t')
        assert attr.deserialize(serialized) == '\\t'

        serialized = attr.serialize('"')
        assert attr.deserialize(serialized) == '"'

    def test_json_attribute(self):
        """
        JSONAttribute.default
        """
        attr = JSONAttribute()
        assert attr is not None

        assert attr.attr_type == STRING
        attr = JSONAttribute(default={})
        assert attr.default == {}

    def test_json_serialize(self):
        """
        JSONAttribute.serialize
        """
        attr = JSONAttribute()
        item = {'foo': 'bar', 'bool': True, 'number': 3.141}
        assert attr.serialize(item) == six.u(json.dumps(item))
        assert attr.serialize({}) == six.u('{}')
        assert attr.serialize(None) is None

    def test_json_deserialize(self):
        """
        JSONAttribute.deserialize
        """
        attr = JSONAttribute()
        item = {'foo': 'bar', 'bool': True, 'number': 3.141}
        encoded = six.u(json.dumps(item))
        assert attr.deserialize(encoded) == item

    def test_control_chars(self):
        """
        JSONAttribute with control chars
        """
        attr = JSONAttribute()
        item = {'foo\t': 'bar\n', 'bool': True, 'number': 3.141}
        encoded = six.u(json.dumps(item))
        assert attr.deserialize(encoded) == item


class TestMapAttribute:
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
        assert attr.deserialize(serialized) == person_attribute

    # Special case for raw map attributes
    def test_null_attribute_raw_map(self):
        null_attribute = {
            'skip': None
        }
        attr = MapAttribute()
        serialized = attr.serialize(null_attribute)
        assert serialized == {'skip': {'NULL': True}}

    def test_null_attribute_subclassed_map(self):
        null_attribute = {
            'map_field': None
        }
        attr = DefaultsMap()
        serialized = attr.serialize(null_attribute)
        assert serialized == {}

    def test_null_attribute_map_after_serialization(self):
        null_attribute = {
            'string_field': '',
        }
        attr = DefaultsMap()
        serialized = attr.serialize(null_attribute)
        assert serialized == {}

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
        assert attr.deserialize(serialized) == attribute

    def test_map_overridden_attrs_accessors(self):
        attr = CustomAttrMap(**{
            'overridden_number_attr': 10,
            'overridden_unicode_attr': "Hello"
        })

        assert attr.overridden_number_attr == 10
        assert attr.overridden_unicode_attr == "Hello"

    def test_map_overridden_attrs_serialize(self):
        attribute = {
            'overridden_number_attr': 10,
            'overridden_unicode_attr': "Hello"
        }
        expected = {'number_attr': {'N': '10'}, 'unicode_attr': {'S': six.u('Hello')}}
        assert CustomAttrMap().serialize(attribute) == expected

    def test_additional_attrs_deserialize(self):
        raw_data = {
            'number_attr': {
                'N': '10'},
            'unicode_attr': {
                'S': six.u('Hello')
            },
            'undeclared_attr': {
                'S': six.u('Goodbye')
            }
        }
        expected = {
            'overridden_number_attr': 10,
            'overridden_unicode_attr': "Hello"
        }
        assert CustomAttrMap().deserialize(raw_data).attribute_values == expected

    def test_defaults(self):
        item = DefaultsMap()
        assert item.validate()
        assert DefaultsMap().serialize(item) == {
            'map_field': {
                'M': {}
            }
        }

    def test_raw_set_attr(self):
        item = AttributeTestModel()
        item.map_attr = {}
        item.map_attr.foo = 'bar'
        item.map_attr.num = 3
        item.map_attr.nested = {'nestedfoo': 'nestedbar'}

        assert item.map_attr['foo'] == 'bar'
        assert item.map_attr['num'] == 3
        assert item.map_attr['nested']['nestedfoo'] == 'nestedbar'

    def test_raw_set_item(self):
        item = AttributeTestModel()
        item.map_attr = {}
        item.map_attr['foo'] = 'bar'
        item.map_attr['num'] = 3
        item.map_attr['nested'] = {'nestedfoo': 'nestedbar'}

        assert item.map_attr['foo'] == 'bar'
        assert item.map_attr['num'] == 3
        assert item.map_attr['nested']['nestedfoo'] == 'nestedbar'

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

        assert item.map_attr['foo'] == 'bar'
        assert item.map_attr['num'] == 3

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
            assert attr[k] == v

    def test_raw_map_iter(self):
        raw = {
            "foo": "bar",
            "num": 3,
            "nested": {
                "nestedfoo": "nestedbar"
            }
        }
        attr = MapAttribute(**raw)

        assert list(iter(raw)) == list(iter(attr))

    def test_raw_map_json_serialize(self):
        raw = {
            "foo": "bar",
            "num": 3,
            "nested": {
                "nestedfoo": "nestedbar"
            }
        }

        serialized_raw = json.dumps(raw, sort_keys=True)
        serialized_attr_from_raw = json.dumps(
            AttributeTestModel(map_attr=raw).map_attr.as_dict(),
            sort_keys=True)
        serialized_attr_from_map = json.dumps(
            AttributeTestModel(map_attr=MapAttribute(**raw)).map_attr.as_dict(),
            sort_keys=True)

        assert serialized_attr_from_raw == serialized_raw
        assert serialized_attr_from_map == serialized_raw

    def test_typed_and_raw_map_json_serialize(self):
        class TypedMap(MapAttribute):
            map_attr = MapAttribute()

        class SomeModel(Model):
            typed_map = TypedMap()

        item = SomeModel(
            typed_map=TypedMap(map_attr={'foo': 'bar'})
        )

        assert json.dumps({'map_attr': {'foo': 'bar'}}) == json.dumps(item.typed_map.as_dict())

    def test_json_serialize(self):
        class JSONMapAttribute(MapAttribute):
            arbitrary_data = JSONAttribute()

            def __eq__(self, other):
                return self.arbitrary_data == other.arbitrary_data

        item = {'foo': 'bar', 'bool': True, 'number': 3.141}
        json_map = JSONMapAttribute(arbitrary_data=item)
        serialized = json_map.serialize(json_map)
        deserialized = json_map.deserialize(serialized)
        assert isinstance(deserialized, JSONMapAttribute)
        assert deserialized == json_map
        assert deserialized.arbitrary_data == item

    def test_serialize_datetime(self):
        class CustomMapAttribute(MapAttribute):
            date_attr = UTCDateTimeAttribute()

        cm = CustomMapAttribute(date_attr=datetime(2017, 1, 1))
        serialized_datetime = cm.serialize(cm)
        expected_serialized_value = {
            'date_attr': {
                'S': u'2017-01-01T00:00:00.000000+0000'
            }
        }
        assert serialized_datetime == expected_serialized_value

    def test_complex_map_accessors(self):
        class NestedThing(MapAttribute):
            double_nested = MapAttribute()
            double_nested_renamed = MapAttribute(attr_name='something_else')

        class ThingModel(Model):
            nested = NestedThing()

        t = ThingModel(nested=NestedThing(
            double_nested={'hello': 'world'},
            double_nested_renamed={'foo': 'bar'})
        )

        assert t.nested.double_nested.as_dict() == {'hello': 'world'}
        assert t.nested.double_nested_renamed.as_dict() == {'foo': 'bar'}
        assert t.nested.double_nested.hello == 'world'
        assert t.nested.double_nested_renamed.foo == 'bar'
        assert t.nested['double_nested'].as_dict() == {'hello': 'world'}
        assert t.nested['double_nested_renamed'].as_dict() == {'foo': 'bar'}
        assert t.nested['double_nested']['hello'] == 'world'
        assert t.nested['double_nested_renamed']['foo'] == 'bar'

        with pytest.raises(AttributeError):
            bad = t.nested.double_nested.bad
        with pytest.raises(AttributeError):
            bad = t.nested.bad
        with pytest.raises(AttributeError):
            bad = t.nested.something_else
        with pytest.raises(KeyError):
            bad = t.nested.double_nested['bad']
        with pytest.raises(KeyError):
            bad = t.nested['something_else']

    def test_metaclass(self):
        assert type(MapAttribute) == MapAttributeMeta

    def test_attribute_paths_subclassing(self):
        class SubMapAttribute(MapAttribute):
            foo = UnicodeAttribute(attr_name='dyn_foo')

        class SubSubMapAttribute(SubMapAttribute):
            bar = UnicodeAttribute(attr_name='dyn_bar')

        class SubModel(Model):
            sub_map = SubMapAttribute(attr_name='dyn_sub_map')

        class SubSubModel(SubModel):
            sub_sub_map = SubSubMapAttribute()

        assert SubModel.sub_map.foo.attr_name == 'dyn_foo'
        assert SubModel.sub_map.foo.attr_path == ['dyn_sub_map', 'dyn_foo']
        assert SubSubModel.sub_map.foo.attr_name == 'dyn_foo'
        assert SubSubModel.sub_map.foo.attr_path == ['dyn_sub_map', 'dyn_foo']
        assert SubSubModel.sub_sub_map.foo.attr_name == 'dyn_foo'
        assert SubSubModel.sub_sub_map.foo.attr_path == ['sub_sub_map', 'dyn_foo']
        assert SubSubModel.sub_sub_map.bar.attr_name == 'dyn_bar'
        assert SubSubModel.sub_sub_map.bar.attr_path == ['sub_sub_map', 'dyn_bar']

    def test_attribute_paths_wrapping(self):
        class InnerMapAttribute(MapAttribute):
            map_attr = MapAttribute(attr_name='dyn_map_attr')

        class MiddleMapAttributeA(MapAttribute):
            inner_map = InnerMapAttribute(attr_name='dyn_in_map_a')

        class MiddleMapAttributeB(MapAttribute):
            inner_map = InnerMapAttribute(attr_name='dyn_in_map_b')

        class OuterMapAttribute(MapAttribute):
            mid_map_a = MiddleMapAttributeA()
            mid_map_b = MiddleMapAttributeB()

        class MyModel(Model):
            outer_map = OuterMapAttribute(attr_name='dyn_out_map')

        mid_map_a_map_attr = MyModel.outer_map.mid_map_a.inner_map.map_attr
        mid_map_b_map_attr = MyModel.outer_map.mid_map_b.inner_map.map_attr

        assert mid_map_a_map_attr.attr_name == 'dyn_map_attr'
        assert mid_map_a_map_attr.attr_path == ['dyn_out_map', 'mid_map_a', 'dyn_in_map_a', 'dyn_map_attr']
        assert mid_map_b_map_attr.attr_name == 'dyn_map_attr'
        assert mid_map_b_map_attr.attr_path == ['dyn_out_map', 'mid_map_b', 'dyn_in_map_b', 'dyn_map_attr']


class TestValueDeserialize:
    def test__get_value_for_deserialize(self):
        expected = '3'
        data = {'N': '3'}
        actual = _get_value_for_deserialize(data)
        assert expected == actual

    def test__get_value_for_deserialize_null(self):
        data = {'NULL': 'True'}
        actual = _get_value_for_deserialize(data)
        assert actual is None


class TestMapAndListAttribute:

    def test_map_of_list(self):
        grocery_list = {
            'fruit': ['apple', 'pear', 32],
            'veggies': ['broccoli', 'potatoes', 5]
        }
        serialized = MapAttribute().serialize(grocery_list)
        assert MapAttribute().deserialize(serialized) == grocery_list

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
        assert MapAttribute().deserialize(serialized) == family_attributes

    def test_list_of_map_with_of(self):
        class Person(MapAttribute):
            name = UnicodeAttribute()
            age = NumberAttribute()

            def __lt__(self, other):
                return self.name < other.name

            def __eq__(self, other):
                return (self.name == other.name and
                        self.age == other.age)

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
        assert sorted(deserialized) == sorted(inp)

    def test_list_of_map_with_of_and_custom_attribute(self, mocker):

        class CustomMapAttribute(MapAttribute):
            custom = NumberAttribute()

            def __eq__(self, other):
                return self.custom == other.custom

        serialize_mock = mocker.spy(CustomMapAttribute.custom, 'serialize',)
        deserialize_mock = mocker.spy(CustomMapAttribute.custom, 'deserialize')

        attribute1 = CustomMapAttribute()
        attribute1.custom = 1

        attribute2 = CustomMapAttribute()
        attribute2.custom = 2

        inp = [attribute1, attribute2]

        list_attribute = ListAttribute(default=[], of=CustomMapAttribute)
        serialized = list_attribute.serialize(inp)
        deserialized = list_attribute.deserialize(serialized)

        assert deserialized == inp
        assert serialize_mock.call_args_list == [call(1), call(2)]
        assert deserialize_mock.call_args_list == [call('1'), call('2')]

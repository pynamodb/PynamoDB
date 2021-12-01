"""
pynamodb attributes tests
"""
import calendar
import json

from base64 import b64encode
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from unittest.mock import patch, call
import pytest

from pynamodb.attributes import (
    BinarySetAttribute, BinaryAttribute, DynamicMapAttribute, NumberSetAttribute, NumberAttribute,
    UnicodeAttribute, UnicodeSetAttribute, UTCDateTimeAttribute, BooleanAttribute, MapAttribute, NullAttribute,
    ListAttribute, JSONAttribute, TTLAttribute, VersionAttribute)
from pynamodb.constants import (
    DATETIME_FORMAT, DEFAULT_ENCODING, NUMBER, STRING, STRING_SET, NUMBER_SET, BINARY_SET,
    BINARY, BOOLEAN,
)
from pynamodb.models import Model


class AttributeTestModel(Model):

    class Meta:
        host = 'http://localhost:8000'
        table_name = 'test'

    binary_attr = BinaryAttribute(hash_key=True)
    binary_set_attr = BinarySetAttribute()
    number_attr = NumberAttribute()
    number_set_attr = NumberSetAttribute()
    unicode_attr = UnicodeAttribute()
    unicode_set_attr = UnicodeSetAttribute()
    datetime_attr = UTCDateTimeAttribute()
    bool_attr = BooleanAttribute()
    json_attr = JSONAttribute()
    map_attr = MapAttribute()
    ttl_attr = TTLAttribute()
    null_attr = NullAttribute(null=True)


class CustomAttrMap(MapAttribute):
    overridden_number_attr = NumberAttribute(attr_name="number_attr")
    overridden_unicode_attr = UnicodeAttribute(attr_name="unicode_attr")


class DefaultsMap(MapAttribute):
    map_field = MapAttribute(default={})
    string_set_field = UnicodeSetAttribute(null=True)


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
        self.instance.binary_set_attr = {b'test', b'test2'}
        assert self.instance.binary_set_attr == {b'test', b'test2'}

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
        self.instance.number_set_attr = {1, 2}
        assert self.instance.number_set_attr == {1, 2}

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
        self.instance.unicode_set_attr = {u"test", u"test2"}
        assert self.instance.unicode_set_attr == {u"test", u"test2"}

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

    def setup(self):
        self.attr = UTCDateTimeAttribute()
        self.dt = datetime(2047, 1, 6, 8, 21, 30, 2000, tzinfo=timezone.utc)

    def test_utc_datetime_attribute(self):
        """
        UTCDateTimeAttribute.default
        """
        attr = UTCDateTimeAttribute(default=self.dt)
        assert attr.attr_type == STRING
        assert attr.default == self.dt

    def test_utc_date_time_serialize(self):
        """
        UTCDateTimeAttribute.serialize
        """
        assert self.attr.serialize(self.dt) == '2047-01-06T08:21:30.002000+0000'

    def test_utc_date_time_serialize_pre_1000(self):
        dt = self.dt.replace(year=1)
        assert self.attr.serialize(dt) == '0001-01-06T08:21:30.002000+0000'

    def test_utc_date_time_deserialize(self):
        """
        UTCDateTimeAttribute.deserialize
        """
        assert self.attr.deserialize('2047-01-06T08:21:30.002000+0000') == self.dt

    def test_utc_date_time_deserialize_pre_1000_not_padded(self):
        assert self.attr.deserialize('1-01-06T08:21:30.002000+0000') == self.dt.replace(year=1)

    @pytest.mark.parametrize(
        "invalid_string",
        [
            '2047-01-06T08:21:30.002000',       # naive datetime
            '2047-01-06T08:21:30+0000',         # missing microseconds
            '2047-01-06T08:21:30.001+0000',     # shortened microseconds
            '2047-01-06T08:21:30.002000-0000'   # "negative" utc
            '2047-01-06T08:21:30.002000+0030'   # not utc
            '2047-01-06 08:21:30.002000+0000',  # missing separator
            '2.47-01-06T08:21:30.002000+0000',
            'abcd-01-06T08:21:30.002000+0000',
            '2047-ab-06T08:21:30.002000+0000',
            '2047-01-abT08:21:30.002000+0000',
            '2047-01-06Tab:21:30.002000+0000',
            '2047-01-06T08:ab:30.002000+0000',
            '2047-01-06T08:21:ab.002000+0000',
            '2047-01-06T08:21:30.a00000+0000',
        ]
    )
    def test_utc_date_time_invalid(self, invalid_string):
        with pytest.raises(ValueError, match="does not match format"):
            self.attr.deserialize(invalid_string)


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
        assert sorted(attr.serialize({b'foo', b'bar'})) == ['YmFy', 'Zm9v']
        assert attr.serialize({}) is None

    def test_binary_set_round_trip(self):
        """
        BinarySetAttribute round trip
        """
        attr = BinarySetAttribute()
        value = {b'foo', b'bar'}
        serial = attr.serialize(value)
        assert attr.deserialize(serial) == value

    def test_binary_set_deserialize(self):
        """
        BinarySetAttribute.deserialize
        """
        attr = BinarySetAttribute()
        value = {b'foo', b'bar'}
        assert attr.deserialize(
            [b64encode(val).decode(DEFAULT_ENCODING) for val in sorted(value)]
        ) == value

    def test_binary_set_attribute(self):
        """
        BinarySetAttribute.serialize
        """
        attr = BinarySetAttribute()
        assert attr is not None

        attr = BinarySetAttribute(default={b'foo', b'bar'})
        assert attr.default == {b'foo', b'bar'}


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
        assert attr.deserialize([json.dumps(val) for val in sorted({1, 2})]) == {1, 2}

    def test_number_set_serialize(self):
        """
        NumberSetAttribute.serialize
        """
        attr = NumberSetAttribute()
        assert attr.serialize({1, 2}) == [json.dumps(val) for val in sorted({1, 2})]
        assert attr.serialize({}) is None

    def test_number_set_attribute(self):
        """
        NumberSetAttribute.default
        """
        attr = NumberSetAttribute()
        assert attr is not None

        attr = NumberSetAttribute(default={1, 2})
        assert attr.default == {1, 2}


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

        attr = UnicodeAttribute(default='foo')
        assert attr.default == 'foo'

    def test_unicode_serialize(self):
        """
        UnicodeAttribute.serialize
        """
        attr = UnicodeAttribute()
        assert attr.serialize('foo') == 'foo'
        assert attr.serialize('') == ''
        assert attr.serialize(None) is None

    def test_unicode_deserialize(self):
        """
        UnicodeAttribute.deserialize
        """
        attr = UnicodeAttribute()
        assert attr.deserialize('foo') == 'foo'
        assert attr.deserialize(u'foo') == 'foo'
        assert attr.deserialize('') == ''
        assert attr.deserialize(None) is None

    def test_unicode_set_serialize(self):
        """
        UnicodeSetAttribute.serialize
        """
        attr = UnicodeSetAttribute()
        assert attr.attr_type == STRING_SET
        assert attr.serialize({}) is None

        expected = sorted(['foo', 'bar'])
        assert sorted(attr.serialize({'foo', 'bar'})) == expected

        expected = sorted(['True', 'False'])
        assert sorted(attr.serialize({'True', 'False'})) == expected

        expected = sorted(['true', 'false'])
        assert sorted(attr.serialize({'true', 'false'})) == expected

    def test_round_trip_unicode_set(self):
        """
        Round trip a unicode set
        """
        attr = UnicodeSetAttribute()
        orig = {'foo', 'bar'}
        assert orig == attr.deserialize(attr.serialize(orig))

        orig = {'true', 'false'}
        assert orig == attr.deserialize(attr.serialize(orig))

        orig = {'1', '2.8'}
        assert orig == attr.deserialize(attr.serialize(orig))

        orig = {'[1,2,3]', '2.8'}
        assert orig == attr.deserialize(attr.serialize(orig))

    def test_unicode_set_deserialize(self):
        """
        UnicodeSetAttribute.deserialize
        """
        attr = UnicodeSetAttribute()
        value = {'foo', 'bar'}
        assert attr.deserialize(value) == value

        value = {'True', 'False'}
        assert attr.deserialize(value) == value

        value = {'true', 'false'}
        assert attr.deserialize(value) == value

        value = {'1', '2.8'}
        assert attr.deserialize(value) == value

    def test_unicode_set_attribute(self):
        """
        UnicodeSetAttribute.default
        """
        attr = UnicodeSetAttribute()
        assert attr is not None
        assert attr.attr_type == STRING_SET
        attr = UnicodeSetAttribute(default={'foo', 'bar'})
        assert attr.default == {'foo', 'bar'}


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
        assert attr.deserialize(True) is True
        assert attr.deserialize(False) is False


class TestTTLAttribute:
    """
    Test TTLAttribute.
    """
    def test_default_and_default_for_new(self):
        with pytest.raises(ValueError, match='An attribute cannot have both default and default_for_new parameters'):
            TTLAttribute(default=timedelta(seconds=1), default_for_new=timedelta(seconds=2))

    @patch('time.time')
    def test_timedelta_ttl(self, mock_time):
        mock_time.side_effect = [1559692800]  # 2019-06-05 00:00:00 UTC
        model = AttributeTestModel()
        model.ttl_attr = timedelta(seconds=60)
        assert model.ttl_attr == datetime(2019, 6, 5, 0, 1, tzinfo=timezone.utc)

    def test_datetime_naive_ttl(self):
        model = AttributeTestModel()
        with pytest.raises(ValueError, match='timezone-aware'):
            model.ttl_attr = datetime(2019, 6, 5, 0, 1)
        assert model.ttl_attr is None

    def test_datetime_with_tz_ttl(self):
        model = AttributeTestModel()
        model.ttl_attr = datetime(2019, 6, 5, 0, 1, tzinfo=timezone.utc)
        assert model.ttl_attr == datetime(2019, 6, 5, 0, 1, tzinfo=timezone.utc)

    def test_ttl_attribute_wrong_type(self):
        with pytest.raises(ValueError, match='TTLAttribute value must be a timedelta or datetime'):
            model = AttributeTestModel()
            model.ttl_attr = 'wrong type'

    @patch('time.time')
    def test_serialize_timedelta(self, mock_time):
        mock_time.side_effect = [1559692800]  # 2019-06-05 00:00:00 UTC
        assert TTLAttribute().serialize(timedelta(seconds=60)) == str(1559692800 + 60)

    def test_serialize_none(self):
        model = AttributeTestModel()
        model.ttl_attr = None
        assert model.ttl_attr == None
        assert TTLAttribute().serialize(model.ttl_attr) == None

    @patch('time.time')
    def test_serialize_deserialize(self, mock_time):
        mock_time.side_effect = [1559692800, 1559692800]  # 2019-06-05 00:00:00 UTC
        model = AttributeTestModel()
        model.ttl_attr = timedelta(minutes=1)
        assert model.ttl_attr == datetime(2019, 6, 5, 0, 1, tzinfo=timezone.utc)
        s = TTLAttribute().serialize(model.ttl_attr)
        assert s == '1559692860'
        assert TTLAttribute().deserialize(s) == datetime(2019, 6, 5, 0, 1, 0, tzinfo=timezone.utc)


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
        assert attr.serialize(item) == json.dumps(item)
        assert attr.serialize({}) == '{}'
        assert attr.serialize(None) is None

    def test_json_deserialize(self):
        """
        JSONAttribute.deserialize
        """
        attr = JSONAttribute()
        item = {'foo': 'bar', 'bool': True, 'number': 3.141}
        encoded = json.dumps(item)
        assert attr.deserialize(encoded) == item

    def test_control_chars(self):
        """
        JSONAttribute with control chars
        """
        attr = JSONAttribute()
        item = {'foo\t': 'bar\n', 'bool': True, 'number': 3.141}
        encoded = json.dumps(item)
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
            'map_field': {},
            'string_set_field': None
        }
        attr = DefaultsMap()
        serialized = attr.serialize(null_attribute)
        assert serialized == {'map_field': {'M': {}}}

    def test_null_attribute_map_after_serialization(self):
        null_attribute = {
            'map_field': {},
            'string_set_field': {},
        }
        attr = DefaultsMap()
        serialized = attr.serialize(null_attribute)
        assert serialized == {'map_field': {'M': {}}}

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
        expected = {'number_attr': {'N': '10'}, 'unicode_attr': {'S': 'Hello'}}
        assert CustomAttrMap().serialize(attribute) == expected

    def test_additional_attrs_deserialize(self):
        raw_data = {
            'number_attr': {
                'N': '10'},
            'unicode_attr': {
                'S': 'Hello'
            },
            'undeclared_attr': {
                'S': 'Goodbye'
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

        for k, v in raw.items():
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

        assert sorted(iter(raw)) == sorted(iter(attr))

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
            key = NumberAttribute(hash_key=True)
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
            key = NumberAttribute(hash_key=True)
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

    def test_attribute_paths_subclassing(self):
        class SubMapAttribute(MapAttribute):
            foo = UnicodeAttribute(attr_name='dyn_foo')

        class SubSubMapAttribute(SubMapAttribute):
            bar = UnicodeAttribute(attr_name='dyn_bar')

        class SubModel(Model):
            key = NumberAttribute(hash_key=True)
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
            key = NumberAttribute(hash_key=True)
            outer_map = OuterMapAttribute(attr_name='dyn_out_map')

        mid_map_a_map_attr = MyModel.outer_map.mid_map_a.inner_map.map_attr
        mid_map_b_map_attr = MyModel.outer_map.mid_map_b.inner_map.map_attr

        assert mid_map_a_map_attr.attr_name == 'dyn_map_attr'
        assert mid_map_a_map_attr.attr_path == ['dyn_out_map', 'mid_map_a', 'dyn_in_map_a', 'dyn_map_attr']
        assert mid_map_b_map_attr.attr_name == 'dyn_map_attr'
        assert mid_map_b_map_attr.attr_path == ['dyn_out_map', 'mid_map_b', 'dyn_in_map_b', 'dyn_map_attr']

    def test_required_elements(self):
        class InnerMapAttribute(MapAttribute):
            foo = UnicodeAttribute()

        class OuterMapAttribute(MapAttribute):
            inner_map = InnerMapAttribute()

        outer_map_attribute = OuterMapAttribute()
        with pytest.raises(ValueError):
            outer_map_attribute.serialize(outer_map_attribute)

        outer_map_attribute = OuterMapAttribute(inner_map={})
        with pytest.raises(ValueError):
            outer_map_attribute.serialize(outer_map_attribute)

        outer_map_attribute = OuterMapAttribute(inner_map=MapAttribute())
        with pytest.raises(ValueError):
            outer_map_attribute.serialize(outer_map_attribute)

        outer_map_attribute = OuterMapAttribute(inner_map={'foo': 'bar'})
        serialized = outer_map_attribute.serialize(outer_map_attribute)
        assert serialized == {'inner_map': {'M': {'foo': {'S': 'bar'}}}}


class TestDynamicMapAttribute:

    class CreatedAtTestModel(Model):
        class CreatedAtMap(DynamicMapAttribute):
            created_at = UTCDateTimeAttribute()
        test_map = CreatedAtMap(default=dict)

    def test_serialize(self):
        test_model = TestDynamicMapAttribute.CreatedAtTestModel()
        test_model.test_map.created_at = datetime(2017, 1, 1, tzinfo=timezone.utc)
        test_model.test_map.foo = 'bar'
        test_model.test_map.empty = None
        assert test_model.serialize() == {'test_map': {'M': {
            'created_at': {'S': '2017-01-01T00:00:00.000000+0000'},
            'foo': {'S': 'bar'},
            'empty': {'NULL': True},
        }}}

    def test_deserialize(self):
        serialized = {'test_map': {'M': {
            'created_at': {'S': '2017-01-01T00:00:00.000000+0000'},
            'foo': {'S': 'bar'},
            'empty': {'NULL': True},
        }}}
        test_model = TestDynamicMapAttribute.CreatedAtTestModel()
        test_model.deserialize(serialized)
        assert test_model.test_map.created_at == datetime(2017, 1, 1, tzinfo=timezone.utc)
        assert test_model.test_map.foo == 'bar'
        assert test_model.test_map.empty is None


class TestListAttribute:

    def test_untyped_list(self):
        untyped_list = [{'Hello': 'World'}, ['!'], {'foo', 'bar'}, None, "", 0, False]
        serialized = ListAttribute().serialize(untyped_list)
        # set attributes are serialized as lists
        untyped_list[2] = list(untyped_list[2])
        assert ListAttribute().deserialize(serialized) == untyped_list

    def test_list_of_strings(self):
        string_list_attribute = ListAttribute(of=UnicodeAttribute)
        string_list = ['foo', 'bar', 'baz']
        serialized = string_list_attribute.serialize(string_list)
        assert string_list_attribute.deserialize(serialized) == string_list

    def test_list_type_error(self):
        string_list_attribute = ListAttribute(of=UnicodeAttribute)

        with pytest.raises(ValueError):
            string_list_attribute.serialize([MapAttribute(foo='bar')])

        with pytest.raises(TypeError):
            string_list_attribute.deserialize([{'M': {'foo': {'S': 'bar'}}}])

    def test_serialize_null(self):
        string_set_list_attribute = ListAttribute(of=UnicodeSetAttribute)
        list_with_empty_set = [{'foo'}, {}, None]
        serialized = string_set_list_attribute.serialize(list_with_empty_set)
        assert string_set_list_attribute.deserialize(serialized) == [{'foo'}, None, None]


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


class TestVersionAttribute:
    def test_serialize(self):
        attr = VersionAttribute()
        assert attr.attr_type == NUMBER
        assert attr.serialize(3.141) == '3'
        assert attr.serialize(1) == '1'
        assert attr.serialize(12345678909876543211234234324234) == '12345678909876543211234234324234'

    def test_deserialize(self):
        attr = VersionAttribute()
        assert attr.deserialize('1') == 1
        assert attr.deserialize('3.141') == 3
        assert attr.deserialize('12345678909876543211234234324234') == 12345678909876543211234234324234


class TestAttributeContainer:
    def test_to_json(self):
        now = datetime.now(tz=timezone.utc)
        now_formatted = now.strftime(DATETIME_FORMAT)
        now_unix_ts = calendar.timegm(now.utctimetuple())
        test_model = AttributeTestModel()
        test_model.binary_attr = b'foo'
        test_model.binary_set_attr = {b'bar'}
        test_model.number_attr = 1
        test_model.number_set_attr = {0, 0.5, 1}
        test_model.unicode_attr = 'foo'
        test_model.unicode_set_attr = {'baz'}
        test_model.datetime_attr = now
        test_model.bool_attr = True
        test_model.json_attr = {'foo': 'bar'}
        test_model.map_attr = {'foo': 'bar'}
        test_model.ttl_attr = now
        test_model.null_attr = True
        assert test_model.to_json() == (
            '{'
            '"binary_attr": "Zm9v", '
            '"binary_set_attr": ["YmFy"], '
            '"bool_attr": true, '
            '"datetime_attr": "' + now_formatted + '", '
            '"json_attr": "{\\"foo\\": \\"bar\\"}", '
            '"map_attr": {"foo": "bar"}, '
            '"null_attr": null, '
            '"number_attr": 1, '
            '"number_set_attr": [0, 0.5, 1], '
            '"ttl_attr": ' + str(now_unix_ts) + ', '
            '"unicode_attr": "foo", '
            '"unicode_set_attr": ["baz"]'
            '}')

    def test_from_json(self):
        now = datetime.now(tz=timezone.utc)
        now_formatted = now.strftime(DATETIME_FORMAT)
        now_unix_ts = calendar.timegm(now.utctimetuple())
        json_string = (
            '{'
            '"binary_attr": "Zm9v", '
            '"binary_set_attr": ["YmFy"], '
            '"bool_attr": true, '
            '"datetime_attr": "' + now_formatted + '", '
            '"json_attr": "{\\"foo\\": \\"bar\\"}", '
            '"map_attr": {"foo": "bar"}, '
            '"null_attr": null, '
            '"number_attr": 1, '
            '"number_set_attr": [0, 0.5, 1], '
            '"ttl_attr": ' + str(now_unix_ts) + ', '
            '"unicode_attr": "foo", '
            '"unicode_set_attr": ["baz"]'
            '}')
        test_model = AttributeTestModel()
        test_model.from_json(json_string)
        assert test_model.binary_attr == b'foo'
        assert test_model.binary_set_attr == {b'bar'}
        assert test_model.number_attr == 1
        assert test_model.number_set_attr == {0, 0.5, 1}
        assert test_model.unicode_attr == 'foo'
        assert test_model.unicode_set_attr == {'baz'}
        assert test_model.datetime_attr == now
        assert test_model.bool_attr is True
        assert test_model.json_attr == {'foo': 'bar'}
        assert test_model.map_attr.foo == 'bar'
        assert test_model.ttl_attr == now.replace(microsecond=0)
        assert test_model.null_attr is None

    def test_to_json_complex(self):
        class MyMap(MapAttribute):
            foo = UnicodeSetAttribute(attr_name='bar')

        class ListTestModel(Model):
            class Meta:
                host = 'http://localhost:8000'
                table_name = 'test'
            unicode_attr = UnicodeAttribute(hash_key=True)
            list_attr = ListAttribute(of=NumberSetAttribute)
            list_map_attr = ListAttribute(of=MyMap)

        list_test_model = ListTestModel()
        list_test_model.unicode_attr = 'foo'
        list_test_model.list_attr = [{0, 1, 2}]
        list_test_model.list_map_attr = [MyMap(foo={'baz'})]
        assert list_test_model.to_json() == (
            '{'
            '"list_attr": [[0, 1, 2]], '
            '"list_map_attr": [{"bar": ["baz"]}], '
            '"unicode_attr": "foo"'
            '}')

    def test_from_json_complex(self):
        class MyMap(MapAttribute):
            foo = UnicodeSetAttribute(attr_name='bar')

        class ListTestModel(Model):
            class Meta:
                host = 'http://localhost:8000'
                table_name = 'test'
            unicode_attr = UnicodeAttribute(hash_key=True)
            list_attr = ListAttribute(of=NumberSetAttribute)
            list_map_attr = ListAttribute(of=MyMap)

        json_string = (
            '{'
            '"list_attr": [[0, 1, 2]], '
            '"list_map_attr": [{"bar": ["baz"]}], '
            '"unicode_attr": "foo"'
            '}')
        list_test_model = ListTestModel()
        list_test_model.from_json(json_string)
        assert list_test_model.unicode_attr == 'foo'
        assert list_test_model.list_attr == [{0, 1, 2}]
        assert list_test_model.list_map_attr[0].foo == {'baz'}

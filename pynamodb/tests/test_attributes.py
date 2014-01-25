"""
pynamodb attributes tests
"""
import six
import json
from base64 import b64encode
from datetime import datetime
from delorean import Delorean
from unittest import TestCase
from pynamodb.constants import UTC, DATETIME_FORMAT
from pynamodb.attributes import (
    BinarySetAttribute, BinaryAttribute, NumberSetAttribute, NumberAttribute,
    UnicodeAttribute, UnicodeSetAttribute, UTCDateTimeAttribute, DEFAULT_ENCODING)


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
        self.assertEqual(
            attr.serialize({b'foo', b'bar'}),
            [b64encode(val).decode(DEFAULT_ENCODING) for val in sorted({b'foo', b'bar'})])
        self.assertEqual(attr.serialize(None), None)

    def test_binary_set_round_trip(self):
        """
        BinarySetAttribute round trip
        """
        attr = BinarySetAttribute()
        value = {b'foo', b'bar'}
        serial = attr.serialize(value)
        self.assertEqual(attr.deserialize(serial), value)

    def test_binary_set_deserialize(self):
        """
        BinarySetAttribute.deserialize
        """
        attr = BinarySetAttribute()
        value = {b'foo', b'bar'}
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

        attr = BinarySetAttribute(default={b'foo', b'bar'})
        self.assertEqual(attr.default, {b'foo', b'bar'})


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

        attr = NumberAttribute(default=1)
        self.assertEqual(attr.default, 1)

    def test_number_serialize(self):
        """
        NumberAttribute.serialize
        """
        attr = NumberAttribute()
        self.assertEqual(attr.serialize(3.141), '3.141')
        self.assertEqual(attr.serialize(1), '1')

    def test_number_deserialize(self):
        """
        NumberAttribute.deserialize
        """
        attr = NumberAttribute()
        self.assertEqual(attr.deserialize('1'), 1)
        self.assertEqual(attr.deserialize('3.141'), 3.141)

    def test_number_set_deserialize(self):
        """
        NumberSetAttribute.deserialize
        """
        attr = NumberSetAttribute()
        self.assertEqual(attr.deserialize([json.dumps(val) for val in sorted({1, 2})]), {1, 2})

    def test_number_set_serialize(self):
        """
        NumberSetAttribute.serialize
        """
        attr = NumberSetAttribute()
        self.assertEqual(attr.serialize({1, 2}), [json.dumps(val) for val in sorted({1, 2})])
        self.assertEqual(attr.serialize(None), None)

    def test_number_set_attribute(self):
        """
        NumberSetAttribute.default
        """
        attr = NumberSetAttribute()
        self.assertIsNotNone(attr)

        attr = NumberSetAttribute(default={1, 2})
        self.assertEqual(attr.default, {1, 2})


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

        attr = UnicodeAttribute(default=six.u('foo'))
        self.assertEqual(attr.default, six.u('foo'))

    def test_unicode_serialize(self):
        """
        UnicodeAttribute.serialize
        """
        attr = UnicodeAttribute()
        self.assertEqual(attr.serialize('foo'), six.u('foo'))
        self.assertEqual(attr.serialize(u'foo'), six.u('foo'))

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
        self.assertEqual(attr.deserialize(None), None)
        self.assertEqual(
            attr.serialize({six.u('foo'), six.u('bar')}),
            [json.dumps(val) for val in sorted({six.u('foo'), six.u('bar')})])

    def test_unicode_set_deserialize(self):
        """
        UnicodeSetAttribute.deserialize
        """
        attr = UnicodeSetAttribute()
        self.assertEqual(
            attr.deserialize([json.dumps(val) for val in sorted({six.u('foo'), six.u('bar')})]),
            {six.u('foo'), six.u('bar')}
        )

    def test_unicode_set_attribute(self):
        """
        UnicodeSetAttribute.default
        """
        attr = UnicodeSetAttribute()
        self.assertIsNotNone(attr)

        attr = UnicodeSetAttribute(default={six.u('foo'), six.u('bar')})
        self.assertEqual(attr.default, {six.u('foo'), six.u('bar')})

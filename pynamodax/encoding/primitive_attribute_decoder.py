from datetime import datetime, timezone

from pynamodax.attributes import (
    Attribute,
    BinaryAttribute,
    BinarySetAttribute,
    JSONAttribute,
    NumberSetAttribute,
    TTLAttribute,
    UnicodeSetAttribute,
    UTCDateTimeAttribute,
)

DECODER_MAPPING = {
    (BinaryAttribute, BinarySetAttribute, JSONAttribute): lambda attr, data: attr.deserialize(data),
    (NumberSetAttribute, UnicodeSetAttribute): lambda _, data: set(data),
    TTLAttribute: lambda _, data: datetime.fromtimestamp(data, tz=timezone.utc),
    UTCDateTimeAttribute: lambda _, data: datetime.fromisoformat(data),
}


class PrimitiveAttributeDecoder:
    @staticmethod
    def decode(attr: Attribute, data):
        for types, callable in DECODER_MAPPING.items():
            if isinstance(attr, types):
                return callable(attr, data)
        return data

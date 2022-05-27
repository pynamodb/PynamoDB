from pynamodax.attributes import (
    Attribute,
    BinaryAttribute,
    BinarySetAttribute,
    DiscriminatorAttribute,
    JSONAttribute,
    TTLAttribute,
    UTCDateTimeAttribute,
)

SERIALIZABLE_TYPES = (BinaryAttribute, BinarySetAttribute, DiscriminatorAttribute, JSONAttribute)
ENCODER_MAPPING = {
    SERIALIZABLE_TYPES: lambda attr, data: attr.serialize(data),
    TTLAttribute: lambda _, data: data.timestamp(),
    UTCDateTimeAttribute: lambda _, data: data.isoformat(),
}


class PrimitiveAttributeEncoder:
    @staticmethod
    def encode(attr: Attribute, data):
        for types, callable in ENCODER_MAPPING.items():
            if isinstance(attr, types):
                return callable(attr, data)
        return data

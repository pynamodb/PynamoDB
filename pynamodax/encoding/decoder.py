from typing import Any, Dict, List, Type, TypeVar, Union

from pynamodax.attributes import (
    Attribute,
    AttributeContainer,
    DiscriminatorAttribute,
    DynamicMapAttribute,
    ListAttribute,
    MapAttribute,
)

from pynamodax.encoding.primitive_attribute_decoder import PrimitiveAttributeDecoder

AC = TypeVar("AC", bound=AttributeContainer)


class Decoder:
    def decode(self, type: Type, data: Dict[str, Any]):
        return self.decode_container(type, data)

    def decode_container(self, type: Type[AC], data: Dict[str, Any]) -> AC:
        attributes = {}
        cls = self.polymorphisize(type, data)
        for name, attr in cls.get_attributes().items():
            if name in data:
                attributes[name] = self.decode_attribute(attr, data[name])
        return cls(**attributes)

    def decode_attribute(self, attr: Attribute, data):
        if isinstance(attr, ListAttribute):
            return self.decode_list(attr, data)
        elif type(attr) == MapAttribute or issubclass(type(attr), MapAttribute):
            return self.decode_map(attr, data)
        else:
            return PrimitiveAttributeDecoder.decode(attr, data)

    def decode_list(self, attr: ListAttribute, data: List) -> List:
        return [self.decode_attribute(self.coerce(attr.element_type), value) for value in data]

    def coerce(self, element_type: Union[Type[Attribute], None]) -> Attribute:
        return (element_type or Attribute)()

    def decode_map(self, attr: MapAttribute, data: Dict[str, Any]):
        if isinstance(attr, MapAttribute) or issubclass(type(attr), MapAttribute):
            return data
        elif isinstance(attr, DynamicMapAttribute) or issubclass(type(attr), DynamicMapAttribute):
            return self.decode_dynamic_map(attr, data)
        else:
            return self.decode_container(type(attr), data)

    def decode_dynamic_map(self, attr: DynamicMapAttribute, data: Dict[str, Any]):
        decoded = {}
        cls = self.polymorphisize(type(attr), data)
        attributes = attr.get_attributes()
        for name, value in data.items():
            if name in attributes:
                decoded[name] = self.decode_attribute(attributes[name], value)
            else:
                decoded[name] = value
        return cls(**decoded)

    def polymorphisize(self, instance_type: Type[AC], data: Dict[str, Any]) -> Type[AC]:
        for name, attr in instance_type.get_attributes().items():
            if isinstance(attr, DiscriminatorAttribute):
                return attr.deserialize(data.pop(name))
        return instance_type

import pytest

from pynamodb.attributes import (
    UnicodeAttribute,
    MapAttribute,
    NumberAttribute
)
from pynamodb.models import Model
from pynamodb.models import Expression
from pynamodb.models import ExpressionContextManager


class CarInfoMap(MapAttribute):
    make = UnicodeAttribute(null=False)
    model = UnicodeAttribute(null=True)


class CarModel(Model):
    class Meta:
        table_name = 'CarModel'
    car_id = NumberAttribute(null=False)
    car_info = CarInfoMap(null=False)


def test_expression_context_manager_generates_expression():
    car_info = CarInfoMap(make='Dodge')
    item = CarModel(car_id=123, car_info=car_info)
    new_value = 'Tesla'
    with item.update_2_cm() as update:
        tesla = update.value(new_value)
        update.le_set('car_info.make', tesla)

    expected_expression = "SET car_info.make = :a"
    expected_attrribute_values = {":a": {"S": new_value}}

    assert True

import mock
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


@mock.patch.object(CarModel, 'update_2')
def test_expression_context_manager_generates_expression(mock_update):
    car_info = CarInfoMap(make='Dodge')
    item = CarModel(car_id=123, car_info=car_info)
    new_value = 'Tesla'
    with item.update_2_cm() as update:
        tesla = update.value(CarInfoMap.make, new_value)
        update.le_set('car_info.make', tesla)


    expected_expression = "SET car_info.make = :a"
    expected_attribute_names = {}
    expected_attribute_values = {":a": {"S": new_value}}

    assert mock_update.call_args_list == [
        mock.call(
            update_expression=expected_expression,
            expression_attribute_names=expected_attribute_names,
            expression_attribute_values=expected_attribute_values,
       )
    ]

    assert True

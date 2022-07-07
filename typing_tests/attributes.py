from __future__ import annotations
from typing import Any

from typing_extensions import assert_type


def test_number_attribute() -> None:
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = NumberAttribute()

    assert_type(MyModel.my_attr, NumberAttribute)
    assert_type(MyModel().my_attr, float)


def test_unicode_attribute() -> None:
    from pynamodb.attributes import UnicodeAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = UnicodeAttribute()

    assert_type(MyModel.my_attr, UnicodeAttribute)
    assert_type(MyModel().my_attr, str)


def test_map_attribute() -> None:
    from pynamodb.attributes import MapAttribute, UnicodeAttribute
    from pynamodb.models import Model

    class MySubMap(MapAttribute):
        s = UnicodeAttribute()

    class MyMap(MapAttribute):
        m2 = MySubMap()

    class MyModel(Model):
        m1 = MyMap()

    assert_type(MyModel.m1, MyMap)
    assert_type(MyModel().m1, MyMap)
    assert_type(MyModel.m1.m2, MySubMap)
    assert_type(MyModel().m1.m2, MySubMap)
    assert_type(MyModel.m1.m2.s, str)
    assert_type(MyModel().m1.m2.s, str)

    assert_type(MyMap.m2, MySubMap)
    assert_type(MyMap().m2, MySubMap)

    assert_type(MySubMap.s, UnicodeAttribute)
    assert_type(MySubMap().s, str)


def test_list_attribute() -> None:
    from pynamodb.attributes import ListAttribute, MapAttribute, UnicodeAttribute
    from pynamodb.models import Model

    class MyMap(MapAttribute):
        my_sub_attr = UnicodeAttribute()

    class MyModel(Model):
        my_list = ListAttribute(of=MyMap)
        my_untyped_list = ListAttribute()  # type: ignore[var-annotated]

    assert_type(MyModel.my_list, ListAttribute[MyMap])
    assert_type(MyModel().my_list, list[MyMap])
    assert_type(MyModel().my_list[0].my_sub_attr, str)

    # Untyped lists are not well-supported yet
    assert_type(MyModel().my_untyped_list[0].my_sub_attr, Any)

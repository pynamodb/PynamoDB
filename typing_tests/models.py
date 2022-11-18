from __future__ import annotations
from typing import Any

from typing_extensions import assert_type


def test_model_count() -> None:
    from pynamodb.models import Model
    from pynamodb.expressions.operand import Path

    class MyModel(Model):
        pass

    assert_type(MyModel.count('hash', Path('a').between(1, 3)), int)


def test_model_query() -> None:
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = NumberAttribute()

    # test conditions
    MyModel.query(123, range_key_condition=(MyModel.my_attr == 5), filter_condition=(MyModel.my_attr == 5))

    # test conditions are optional
    MyModel.query(123, range_key_condition=None, filter_condition=None)


def test_pagination() -> None:
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = NumberAttribute()

    result_iterator = MyModel.query(123)
    for model in result_iterator:
        assert_type(model, MyModel)
    if result_iterator.last_evaluated_key:
        assert_type(result_iterator.last_evaluated_key['my_attr'], dict[str, Any])


def test_model_update() -> None:
    from pynamodb.attributes import NumberAttribute, UnicodeAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = NumberAttribute()
        my_str_attr = UnicodeAttribute()

    my_model = MyModel()
    my_model.update(actions=[
        # test update expressions
        MyModel.my_attr.set(MyModel.my_attr + 123),
        MyModel.my_attr.set(123 + MyModel.my_attr),
        MyModel.my_attr.set(MyModel.my_attr - 123),
        MyModel.my_attr.set(123 - MyModel.my_attr),
        MyModel.my_attr.set(MyModel.my_attr | 123),
    ])


def test_paths() -> None:
    import pynamodb.expressions.operand
    import pynamodb.expressions.condition
    from pynamodb.attributes import ListAttribute, MapAttribute, UnicodeAttribute
    from pynamodb.models import Model

    class MyMap(MapAttribute):
        my_sub_attr = UnicodeAttribute()

    class MyModel(Model):
        my_list = ListAttribute(of=MyMap)
        my_map = MyMap()

    assert_type(MyModel.my_list[0], pynamodb.expressions.operand.Path)
    assert_type(MyModel.my_list[0] == MyModel(), pynamodb.expressions.condition.Comparison)
    # the following string indexing is not type checked - not by mypy nor in runtime
    assert_type(MyModel.my_list[0]['my_sub_attr'] == 'foobar', pynamodb.expressions.condition.Comparison)
    assert_type(MyModel.my_map == 'foobar', pynamodb.expressions.condition.Comparison)


def test_index_query_scan() -> None:
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model
    from pynamodb.indexes import GlobalSecondaryIndex
    from pynamodb.pagination import ResultIterator

    class UntypedIndex(GlobalSecondaryIndex):
        bar = NumberAttribute(hash_key=True)

    class TypedIndex(GlobalSecondaryIndex['MyModel']):
        bar = NumberAttribute(hash_key=True)

    class MyModel(Model):
        foo = NumberAttribute(hash_key=True)
        bar = NumberAttribute()

        untyped_index = UntypedIndex()
        typed_index = TypedIndex()

    # Ensure old code keeps working
    untyped_query_result: ResultIterator = MyModel.untyped_index.query(123)
    assert_type(next(untyped_query_result), Any)

    # Allow users to specify which model their indices return
    typed_query_result: ResultIterator[MyModel] = MyModel.typed_index.query(123)
    assert_type(next(typed_query_result), MyModel)

    # Ensure old code keeps working
    untyped_scan_result = MyModel.untyped_index.scan()
    assert_type(next(untyped_scan_result), Any)

    # Allow users to specify which model their indices return
    typed_scan_result = MyModel.typed_index.scan()
    assert_type(next(typed_scan_result), MyModel)


def test_map_attribute_derivation() -> None:
    from pynamodb.attributes import MapAttribute

    class MyMap(MapAttribute, object):
        pass


def test_is_in() -> None:
    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute

    class MyModel(Model):
        attr = UnicodeAttribute()

    _ = MyModel.attr.is_in('foo', 'bar')
    _ = MyModel.attr.is_in(123)  # type:ignore[arg-type]
    _ = MyModel.attr.is_in(['foo', 'bar'])  # type:ignore[arg-type]


def test_append() -> None:
    from pynamodb.models import Model
    from pynamodb.attributes import ListAttribute, NumberAttribute

    class MyModel(Model):
        attr = ListAttribute(of=NumberAttribute)

    MyModel.attr.append(42)  # type:ignore[arg-type]
    MyModel.attr.append([42])
    MyModel.attr.prepend(42)  # type:ignore[arg-type]
    MyModel.attr.prepend([42])

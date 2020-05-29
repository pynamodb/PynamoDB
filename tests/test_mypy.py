"""
Note: The expected error strings may change in a future version of mypy.
      Please update as needed.
"""


def test_model(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.models import Model
    from pynamodb.expressions.operand import Path

    class MyModel(Model):
        pass

    reveal_type(MyModel.count('hash', Path('a').between(1, 3)))  # N: Revealed type is 'builtins.int'
    """)


def test_model_query(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = NumberAttribute()

    # test conditions
    MyModel.query(123, range_key_condition=(MyModel.my_attr == 5), filter_condition=(MyModel.my_attr == 5))

    # test conditions are optional
    MyModel.query(123, range_key_condition=None, filter_condition=None)
    """)


def test_pagination(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = NumberAttribute()

    result_iterator = MyModel.query(123)
    for model in result_iterator:
        reveal_type(model)  # N: Revealed type is '__main__.MyModel*'
    if result_iterator.last_evaluated_key:
        reveal_type(result_iterator.last_evaluated_key['my_attr'])  # N: Revealed type is 'builtins.dict*[builtins.str, Any]'
    """)


def test_model_update(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = NumberAttribute()

    my_model = MyModel()
    my_model.update(actions=[
        # test update expressions
        MyModel.my_attr.set(MyModel.my_attr + 123),
        MyModel.my_attr.set(123 + MyModel.my_attr),
        MyModel.my_attr.set(MyModel.my_attr - 123),
        MyModel.my_attr.set(123 - MyModel.my_attr),
        MyModel.my_attr.set(MyModel.my_attr | 123),
    ])
    """)  # noqa: E501


def test_number_attribute(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = NumberAttribute()

    reveal_type(MyModel.my_attr)  # N: Revealed type is 'pynamodb.attributes.NumberAttribute'
    reveal_type(MyModel().my_attr)  # N: Revealed type is 'builtins.float*'
    """)


def test_unicode_attribute(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import UnicodeAttribute
    from pynamodb.models import Model

    class MyModel(Model):
        my_attr = UnicodeAttribute()

    reveal_type(MyModel.my_attr)  # N: Revealed type is 'pynamodb.attributes.UnicodeAttribute'
    reveal_type(MyModel().my_attr)  # N: Revealed type is 'builtins.str*'
    """)


def test_map_attribute(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import MapAttribute, UnicodeAttribute
    from pynamodb.models import Model

    class MySubMap(MapAttribute):
        s = UnicodeAttribute()

    class MyMap(MapAttribute):
        m2 = MySubMap()

    class MyModel(Model):
        m1 = MyMap()

    reveal_type(MyModel.m1)  # N: Revealed type is '__main__.MyMap'
    reveal_type(MyModel().m1)  # N: Revealed type is '__main__.MyMap'
    reveal_type(MyModel.m1.m2)  # N: Revealed type is '__main__.MySubMap'
    reveal_type(MyModel().m1.m2)  # N: Revealed type is '__main__.MySubMap'
    reveal_type(MyModel.m1.m2.s)  # N: Revealed type is 'builtins.str*'
    reveal_type(MyModel().m1.m2.s)  # N: Revealed type is 'builtins.str*'

    reveal_type(MyMap.m2)  # N: Revealed type is '__main__.MySubMap'
    reveal_type(MyMap().m2)  # N: Revealed type is '__main__.MySubMap'

    reveal_type(MySubMap.s)  # N: Revealed type is 'pynamodb.attributes.UnicodeAttribute'
    reveal_type(MySubMap().s)  # N: Revealed type is 'builtins.str*'
    """)


def test_list_attribute(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import ListAttribute, MapAttribute, UnicodeAttribute
    from pynamodb.models import Model

    class MyMap(MapAttribute):
        my_sub_attr = UnicodeAttribute()

    class MyModel(Model):
        my_list = ListAttribute(of=MyMap)
        my_untyped_list = ListAttribute()  # E: Need type annotation for 'my_untyped_list'  [var-annotated]

    reveal_type(MyModel.my_list)  # N: Revealed type is 'pynamodb.attributes.ListAttribute[__main__.MyMap]'
    reveal_type(MyModel().my_list)  # N: Revealed type is 'builtins.list*[__main__.MyMap*]'
    reveal_type(MyModel().my_list[0].my_sub_attr)  # N: Revealed type is 'builtins.str*'

    # Untyped lists are not well supported yet
    reveal_type(MyModel().my_untyped_list[0].my_sub_attr)  # N: Revealed type is 'Any'
    """)


def test_paths(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import ListAttribute, MapAttribute, UnicodeAttribute
    from pynamodb.models import Model

    class MyMap(MapAttribute):
        my_sub_attr = UnicodeAttribute()

    class MyModel(Model):
        my_list = ListAttribute(of=MyMap)
        my_map = MyMap()

    reveal_type(MyModel.my_list[0])  # N: Revealed type is 'pynamodb.expressions.operand.Path'
    reveal_type(MyModel.my_list[0] == MyModel())  # N: Revealed type is 'pynamodb.expressions.condition.Comparison'
    # the following string indexing is not type checked - not by mypy nor in runtime
    reveal_type(MyModel.my_list[0]['my_sub_attr'] == 'foobar')  # N: Revealed type is 'pynamodb.expressions.condition.Comparison'
    """)


def test_index_query_scan(assert_mypy_output):
    assert_mypy_output("""
    from pynamodb.attributes import NumberAttribute
    from pynamodb.models import Model
    from pynamodb.indexes import GlobalSecondaryIndex
    from pynamodb.pagination import ResultIterator

    class UntypedIndex(GlobalSecondaryIndex):
        bar = NumberAttribute(hash_key=True)

    class TypedIndex(GlobalSecondaryIndex[MyModel]):
        bar = NumberAttribute(hash_key=True)

    class MyModel(Model):
        foo = NumberAttribute(hash_key=True)
        bar = NumberAttribute()

        untyped_index = UntypedIndex()
        typed_index = TypedIndex()

    # Ensure old code keeps working
    untyped_result: ResultIterator = MyModel.untyped_index.query(123)
    model: MyModel = next(untyped_result)
    not_model: int = next(untyped_result)  # this is legacy behavior so it's "fine"

    # Allow users to specify which model their indices return
    typed_result: ResultIterator[MyModel] = MyModel.typed_index.query(123)
    my_model = next(typed_result)
    not_model = next(typed_result)  # E: Incompatible types in assignment (expression has type "MyModel", variable has type "int")  [assignment]

    # Ensure old code keeps working
    untyped_result = MyModel.untyped_index.scan()
    model = next(untyped_result)
    not_model = next(untyped_result)  # this is legacy behavior so it's "fine"

    # Allow users to specify which model their indices return
    typed_result = MyModel.typed_index.scan()
    model = next(typed_result)
    not_model = next(typed_result)  # E: Incompatible types in assignment (expression has type "MyModel", variable has type "int")  [assignment]
    """)

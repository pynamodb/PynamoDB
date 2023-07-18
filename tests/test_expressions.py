from typing import Any
from typing import Dict
from unittest import TestCase

from pynamodb.attributes import ListAttribute, MapAttribute, NumberSetAttribute, UnicodeAttribute, UnicodeSetAttribute, \
    NumberAttribute
from pynamodb.expressions.condition import Condition, size
from pynamodb.expressions.operand import Path, Value
from pynamodb.expressions.projection import create_projection_expression
from pynamodb.expressions.update import Action, Update


class PathTestCase(TestCase):

    def test_document_path(self):
        path = Path('foo.bar')
        assert str(path) == 'foo.bar'
        assert repr(path) == "Path(['foo', 'bar'])"

    def test_attribute_name(self):
        path = Path(['foo.bar'])
        assert str(path) == "'foo.bar'"
        assert repr(path) == "Path(['foo.bar'])"

    def test_index_document_path(self):
        path = Path('foo.bar')[0]
        assert str(path) == 'foo.bar[0]'
        assert repr(path) == "Path(['foo', 'bar[0]'])"

    def test_index_attribute_name(self):
        path = Path(['foo.bar'])[0]
        assert str(path) == "'foo.bar'[0]"
        assert repr(path) == "Path(['foo.bar[0]'])"

    def test_index_map_attribute(self):
        path = Path(['foo.bar'])['baz']
        assert str(path) == "'foo.bar'.baz"
        assert repr(path) == "Path(['foo.bar', 'baz'])"

    def test_index_invalid(self):
        with self.assertRaises(TypeError):
            _ = Path('foo.bar')[0.0]  # type: ignore


class ActionTestCase(TestCase):

    def test_action(self):
        action = Action(Path('foo.bar'))
        action.format_string = '{0}'
        assert repr(action) == 'foo.bar'

    def test_action_eq(self):
        action = Action(Path('foo.bar'))
        assert action == action

        action_eq = Action(Path('foo.bar'))
        assert action == action_eq

        action_not_eq = Action(Path('spam.ham'))
        assert action != action_not_eq

        attr_s = UnicodeAttribute(attr_name='foo')
        assert attr_s.set('bar') == attr_s.set('bar')
        assert attr_s.set('bar') != attr_s.set('baz')

        attr_n = NumberAttribute(attr_name='num')
        assert attr_n.add(42) == attr_n.add(42)
        assert attr_n.add(42) != attr_n.set(42)
        assert attr_n.add(42) != attr_n.add(7)

        attr_s2 = UnicodeAttribute(attr_name='foo')
        assert attr_s.set('bar') != attr_s2.set('bar')


class ProjectionExpressionTestCase(TestCase):

    def setUp(self):
        self.placeholders: Dict[str, str] = {}

    def test_create_projection_expression(self):
        attributes_to_get = ['Description', 'RelatedItems[0]', 'ProductReviews.FiveStar']
        projection_expression = create_projection_expression(attributes_to_get, self.placeholders)
        assert projection_expression == "#0, #1[0], #2.#3"
        assert self.placeholders == {
            'Description': '#0',
            'RelatedItems': '#1',
            'ProductReviews': '#2',
            'FiveStar': '#3',
        }

    def test_create_projection_expression_repeated_names(self):
        attributes_to_get = ['ProductReviews.FiveStar', 'ProductReviews.ThreeStar', 'ProductReviews.OneStar']
        projection_expression = create_projection_expression(attributes_to_get, self.placeholders)
        assert projection_expression == "#0.#1, #0.#2, #0.#3"
        assert self.placeholders == {'ProductReviews': '#0', 'FiveStar': '#1', 'ThreeStar': '#2', 'OneStar': '#3'}

    def test_create_projection_expression_invalid_attribute_raises(self):
        invalid_attributes = ['', '[0]', 'foo[bar]', 'MyList[-1]', 'MyList[0.4]']
        for attribute in invalid_attributes:
            with self.assertRaises(ValueError):
                create_projection_expression([attribute], {})

    def test_create_project_expression_with_document_paths(self):
        attributes_to_get = [Path('foo.bar')[0]]
        projection_expression = create_projection_expression(attributes_to_get, self.placeholders)
        assert projection_expression == "#0.#1[0]"
        assert self.placeholders == {'foo': '#0', 'bar': '#1'}

    def test_create_project_expression_with_attribute_names(self):
        attributes_to_get = [Path(['foo.bar'])[0]]
        projection_expression = create_projection_expression(attributes_to_get, self.placeholders)
        assert projection_expression == "#0[0]"
        assert self.placeholders == {'foo.bar': '#0'}

    def test_create_projection_expression_with_attributes(self):
        attributes_to_get = [
            UnicodeAttribute(attr_name='ProductReviews.FiveStar'),
            UnicodeAttribute(attr_name='ProductReviews.ThreeStar'),
            UnicodeAttribute(attr_name='ProductReviews.OneStar')
        ]
        projection_expression = create_projection_expression(attributes_to_get, self.placeholders)
        assert projection_expression == "#0, #1, #2"
        assert self.placeholders == {
            'ProductReviews.FiveStar': '#0',
            'ProductReviews.ThreeStar': '#1',
            'ProductReviews.OneStar': '#2',
        }

    def test_create_projection_expression_not_a_list(self):
        attributes_to_get = 'Description'
        projection_expression = create_projection_expression(attributes_to_get, self.placeholders)
        assert projection_expression == "#0"
        assert self.placeholders == {'Description': '#0'}


class ConditionExpressionTestCase(TestCase):

    def setUp(self):
        self.attribute = UnicodeAttribute(attr_name='foo')
        self.placeholder_names: Dict[str, str] = {}
        self.expression_attribute_values: Dict[str, str] = {}

    def test_condition_eq(self):
        condition = self.attribute == 'foo'
        condition_eq = self.attribute == 'foo'
        condition_not_eq = self.attribute == 'bar'
        assert condition == condition_eq
        assert condition != condition_not_eq

        different_attr = UnicodeAttribute(attr_name='foo')
        condition_not_eq = different_attr == 'foo'
        assert condition != condition_not_eq

    def test_equal(self):
        condition = self.attribute == 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_not_equal(self):
        condition = self.attribute != 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 <> :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_less_than(self):
        condition = self.attribute < 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 < :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_less_than_or_equal(self):
        condition = self.attribute <= 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 <= :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_greater_than(self):
        condition = self.attribute > 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 > :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_greater_than_or_equal(self):
        condition = self.attribute >= 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 >= :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_between(self):
        condition = self.attribute.between('bar', 'baz')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 BETWEEN :0 AND :1"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}}

    def test_in(self):
        condition = self.attribute.is_in('bar', 'baz')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 IN (:0, :1)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}}

    def test_exists(self):
        condition = self.attribute.exists()
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "attribute_exists (#0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {}

    def test_does_not_exist(self):
        condition = self.attribute.does_not_exist()
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "attribute_not_exists (#0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {}

    def test_is_type(self):
        condition = self.attribute.is_type()
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "attribute_type (#0, :0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'S'}}

    def test_begins_with(self):
        condition = self.attribute.startswith('bar')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "begins_with (#0, :0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_contains(self):
        condition = self.attribute.contains('bar')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "contains (#0, :0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_contains_string_set(self):
        condition = UnicodeSetAttribute(attr_name='foo').contains('bar')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "contains (#0, :0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_contains_number_set(self):
        condition = NumberSetAttribute(attr_name='foo').contains(1)
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "contains (#0, :0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'N': '1'}}

    def test_contains_list(self):
        condition = ListAttribute(attr_name='foo').contains('bar')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "contains (#0, :0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_contains_attribute(self):
        condition = ListAttribute(attr_name='foo').contains(Path('bar'))
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "contains (#0, #1)"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {}

    def test_size(self):
        condition = size(self.attribute) == 3
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "size (#0) = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'N': '3'}}

    def test_sizes(self):
        condition = size(self.attribute) == size(Path('bar'))
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "size (#0) = size (#1)"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {}

    def test_and(self):
        condition = (self.attribute < 'bar') & (self.attribute > 'baz')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "(#0 < :0 AND #0 > :1)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}}

    def test_invalid_and(self):
        condition = self.attribute < 'bar'
        with self.assertRaises(TypeError):
            condition &= None

    def test_rand(self):
        condition = None
        condition &= self.attribute < 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 < :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_invalid_rand(self):
        condition = 42
        with self.assertRaises(TypeError):
            condition &= self.attribute < 'bar'

    def test_or(self):
        condition = (self.attribute < 'bar') | (self.attribute > 'baz')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "(#0 < :0 OR #0 > :1)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}}

    def test_invalid_or(self):
        condition = self.attribute < 'bar'
        with self.assertRaises(TypeError):
            condition |= None

    def test_not(self):
        condition = ~(self.attribute < 'bar')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "(NOT #0 < :0)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_compound_logic(self):
        condition = (~(self.attribute < 'bar') & (self.attribute > 'baz')) | (self.attribute == 'foo')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "(((NOT #0 < :0) AND #0 > :1) OR #0 = :2)"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}, ':2': {'S': 'foo'}}

    def test_indexing(self):
        condition = ListAttribute(attr_name='foo')[0] == 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0[0] = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_invalid_indexing(self):
        with self.assertRaises(TypeError):
            _ = self.attribute[0]

    def test_double_indexing(self):
        condition = ListAttribute(attr_name='foo')[0][1] == 'bar'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0[0][1] = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_typed_list_indexing(self):
        class StringMap(MapAttribute):
            bar = UnicodeAttribute()
        condition = ListAttribute(attr_name='foo', of=StringMap)[0].bar == 'baz'  # type: ignore
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0[0].#1 = :0"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {':0': {'S': 'baz'}}

    def test_map_comparison(self):
        # Simulate initialization from inside an AttributeContainer
        my_map_attribute = MapAttribute[str, str](attr_name='foo')
        my_map_attribute._make_attribute()
        my_map_attribute._update_attribute_paths(my_map_attribute.attr_name)

        condition = my_map_attribute == MapAttribute(bar='baz')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'M': {'bar': {'S': 'baz'}}}}

    def test_map_comparison_rhs(self):
        # Simulate initialization from inside an AttributeContainer
        my_map_attribute = MapAttribute[str, str](attr_name='foo')
        my_map_attribute._make_attribute()
        my_map_attribute._update_attribute_paths(my_map_attribute.attr_name)

        condition = MapAttribute(bar='baz') == my_map_attribute
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'M': {'bar': {'S': 'baz'}}}}

    def test_list_comparison(self):
        condition = ListAttribute(attr_name='foo') == ['bar', 'baz']
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'L': [{'S': 'bar'}, {'S': 'baz'}]}}

    def test_dotted_attribute_name(self):
        self.attribute.attr_name = 'foo.bar'
        condition = self.attribute == 'baz'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0"
        assert self.placeholder_names == {'foo.bar': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'baz'}}

    def test_map_attribute_indexing(self):
        # Simulate initialization from inside an AttributeContainer
        my_map_attribute = MapAttribute[str, str](attr_name='foo.bar')
        my_map_attribute._make_attribute()
        my_map_attribute._update_attribute_paths(my_map_attribute.attr_name)

        condition: Condition = my_map_attribute['foo'] == 'baz'  # type: ignore
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0.#1 = :0"
        assert self.placeholder_names == {'foo.bar': '#0', 'foo': '#1'}
        assert self.expression_attribute_values == {':0': {'S': 'baz'}}

    def test_map_attribute_dereference(self):
        class MyMapAttribute(MapAttribute):
            nested_string = self.attribute

        # Simulate initialization from inside an AttributeContainer
        my_map_attribute = MyMapAttribute(attr_name='foo.bar')
        my_map_attribute._make_attribute()
        my_map_attribute._update_attribute_paths(my_map_attribute.attr_name)

        condition: Condition = my_map_attribute.nested_string == 'baz'  # type: ignore
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0.#1 = :0"
        assert self.placeholder_names == {'foo.bar': '#0', 'foo': '#1'}
        assert self.expression_attribute_values == {':0': {'S': 'baz'}}

    def test_map_attribute_dereference_via_indexing(self):
        class MyMapAttribute(MapAttribute):
            nested_string = self.attribute

        # Simulate initialization from inside an AttributeContainer
        my_map_attribute = MyMapAttribute(attr_name='foo.bar')
        my_map_attribute._make_attribute()
        my_map_attribute._update_attribute_paths(my_map_attribute.attr_name)

        condition = my_map_attribute['nested_string'] == 'baz'
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0.#1 = :0"
        assert self.placeholder_names == {'foo.bar': '#0', 'foo': '#1'}
        assert self.expression_attribute_values == {':0': {'S': 'baz'}}

    def test_map_attribute_dereference_via_indexing_missing_attribute(self):
        class MyMapAttribute(MapAttribute):
            nested_string = self.attribute

        # Simulate initialization from inside an AttributeContainer
        my_map_attribute = MyMapAttribute(attr_name='foo.bar')
        my_map_attribute._make_attribute()
        my_map_attribute._update_attribute_paths(my_map_attribute.attr_name)

        with self.assertRaises(AttributeError):
            _ = my_map_attribute['missing_attribute'] == 'baz'

    def test_attribute_comparison(self):
        condition = self.attribute == UnicodeAttribute(attr_name='bar')
        expression = condition.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = #1"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {}


class UpdateExpressionTestCase(TestCase):

    def setUp(self):
        self.attribute = UnicodeAttribute(attr_name='foo')
        self.set_attribute = NumberSetAttribute(attr_name='foo_set')
        self.list_attribute = ListAttribute[Any](attr_name='foo_list')
        self.placeholder_names: Dict[str, str] = {}
        self.expression_attribute_values: Dict[str, str] = {}

    def test_set_action(self):
        action = self.attribute.set('bar')
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'S': 'bar'}}

    def test_set_action_as_remove(self):
        action = self.set_attribute.set([])
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0"
        assert self.placeholder_names == {'foo_set': '#0'}
        assert self.expression_attribute_values == {}

    def test_set_action_attribute_container(self):
        # Simulate initialization from inside an AttributeContainer
        my_map_attribute = MapAttribute[str, str](attr_name='foo')
        my_map_attribute._make_attribute()
        my_map_attribute._update_attribute_paths(my_map_attribute.attr_name)

        action = my_map_attribute.set(MapAttribute(bar='baz'))
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'M': {'bar': {'S': 'baz'}}}}

    def test_increment_action(self):
        action = self.attribute.set(Path('bar') + 0)
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = #1 + :0"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {':0': {'N': '0'}}

    def test_increment_action_value(self):
        action = self.attribute.set(Value(0) + Path('bar'))
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0 + #1"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {':0': {'N': '0'}}

    def test_decrement_action(self):
        action = self.attribute.set(Path('bar') - 0)
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = #1 - :0"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {':0': {'N': '0'}}

    def test_decrement_action_value(self):
        action = self.attribute.set(Value(0) - Path('bar'))
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = :0 - #1"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {':0': {'N': '0'}}

    def test_append_action(self):
        action = self.attribute.set(Path('bar').append(['baz']))
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = list_append (#1, :0)"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {':0': {'L': [{'S': 'baz'}]}}

    def test_prepend_action(self):
        action = self.attribute.set(Path('bar').prepend(['baz']))
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = list_append (:0, #1)"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {':0': {'L': [{'S': 'baz'}]}}

    def test_conditional_set_action(self):
        action = self.attribute.set(Path('bar') | 'baz')
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 = if_not_exists (#1, :0)"
        assert self.placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert self.expression_attribute_values == {':0': {'S': 'baz'}}

    def test_remove_action(self):
        action = self.attribute.remove()
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {}

    def test_remove_action_list_element(self):
        action = self.list_attribute[10].remove()
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0[10]"
        assert self.placeholder_names == {'foo_list': '#0'}
        assert self.expression_attribute_values == {}

    def test_add_action(self):
        action = Path('foo').add(0)
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'N': '0'}}

    def test_add_action_set(self):
        action = NumberSetAttribute(attr_name='foo').add(0, 1)
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'NS': ['0', '1']}}

    def test_add_action_serialized(self):
        action = NumberSetAttribute(attr_name='foo').add({'NS': ['0']})
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'NS': ['0']}}

    def test_add_action_list(self):
        with self.assertRaises(ValueError):
            Path('foo').add({'L': [{'N': '0'}]})

    def test_delete_action(self):
        action = NumberSetAttribute(attr_name='foo').delete(0, 1)
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'NS': ['0', '1']}}

    def test_delete_action_set(self):
        action = NumberSetAttribute(attr_name='foo').delete({0, 1})
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'NS': ['0', '1']}}

    def test_delete_action_serialized(self):
        action = NumberSetAttribute(attr_name='foo').delete({'NS': ['0']})
        expression = action.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "#0 :0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {':0': {'NS': ['0']}}

    def test_delete_action_non_set(self):
        with self.assertRaises(ValueError):
            Path('foo').delete({'N': '0'})

    def test_update(self):
        update = Update(
            self.attribute.set({'S': 'bar'}),
            self.attribute.remove(),
            self.set_attribute.add({'NS': ['0']}),
            self.set_attribute.delete({'NS': ['1']})
        )
        expression = update.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "SET #0 = :0 REMOVE #0 ADD #1 :1 DELETE #1 :2"
        assert self.placeholder_names == {'foo': '#0', 'foo_set': '#1'}
        assert self.expression_attribute_values == {
            ':0': {'S': 'bar'},
            ':1': {'NS': ['0']},
            ':2': {'NS': ['1']}
        }

    def test_update_set_to_empty(self):
        update = Update(
            self.set_attribute.set([]),
        )
        expression = update.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "REMOVE #0"
        assert self.placeholder_names == {'foo_set': '#0'}
        assert self.expression_attribute_values == {}

    def test_update_skips_empty_clauses(self):
        update = Update(self.attribute.remove())
        expression = update.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression == "REMOVE #0"
        assert self.placeholder_names == {'foo': '#0'}
        assert self.expression_attribute_values == {}

    def test_update_empty(self):
        update = Update()
        expression = update.serialize(self.placeholder_names, self.expression_attribute_values)
        assert expression is None
        assert self.placeholder_names == {}
        assert self.expression_attribute_values == {}

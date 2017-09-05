from pynamodb.attributes import ListAttribute, MapAttribute, NumberSetAttribute, UnicodeAttribute, UnicodeSetAttribute
from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.expressions.condition import size
from pynamodb.expressions.operand import Path
from pynamodb.expressions.projection import create_projection_expression
from pynamodb.expressions.update import Update


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

    def test_index_invalid(self):
        with self.assertRaises(TypeError):
            Path('foo.bar')['foo']


class ProjectionExpressionTestCase(TestCase):

    def test_create_projection_expression(self):
        attributes_to_get = ['Description', 'RelatedItems[0]', 'ProductReviews.FiveStar']
        placeholders = {}
        projection_expression = create_projection_expression(attributes_to_get, placeholders)
        assert projection_expression == "#0, #1[0], #2.#3"
        assert placeholders == {'Description': '#0', 'RelatedItems': '#1', 'ProductReviews': '#2', 'FiveStar': '#3'}

    def test_create_projection_expression_repeated_names(self):
        attributes_to_get = ['ProductReviews.FiveStar', 'ProductReviews.ThreeStar', 'ProductReviews.OneStar']
        placeholders = {}
        projection_expression = create_projection_expression(attributes_to_get, placeholders)
        assert projection_expression == "#0.#1, #0.#2, #0.#3"
        assert placeholders == {'ProductReviews': '#0', 'FiveStar': '#1', 'ThreeStar': '#2', 'OneStar': '#3'}

    def test_create_projection_expression_invalid_attribute_raises(self):
        invalid_attributes = ['', '[0]', 'foo[bar]', 'MyList[-1]', 'MyList[0.4]']
        for attribute in invalid_attributes:
            with self.assertRaises(ValueError):
                create_projection_expression([attribute], {})

    def test_create_project_expression_with_document_paths(self):
        attributes_to_get = [Path('foo.bar')[0]]
        placeholders = {}
        projection_expression = create_projection_expression(attributes_to_get, placeholders)
        assert projection_expression == "#0.#1[0]"
        assert placeholders == {'foo': '#0', 'bar': '#1'}

    def test_create_project_expression_with_attribute_names(self):
        attributes_to_get = [Path(['foo.bar'])[0]]
        placeholders = {}
        projection_expression = create_projection_expression(attributes_to_get, placeholders)
        assert projection_expression == "#0[0]"
        assert placeholders == {'foo.bar': '#0'}

    def test_create_projection_expression_with_attributes(self):
        attributes_to_get = [
            UnicodeAttribute(attr_name='ProductReviews.FiveStar'),
            UnicodeAttribute(attr_name='ProductReviews.ThreeStar'),
            UnicodeAttribute(attr_name='ProductReviews.OneStar')
        ]
        placeholders = {}
        projection_expression = create_projection_expression(attributes_to_get, placeholders)
        assert projection_expression == "#0, #1, #2"
        assert placeholders == {
            'ProductReviews.FiveStar': '#0',
            'ProductReviews.ThreeStar': '#1',
            'ProductReviews.OneStar': '#2',
        }

    def test_create_projection_expression_not_a_list(self):
        attributes_to_get = 'Description'
        placeholders = {}
        projection_expression = create_projection_expression(attributes_to_get, placeholders)
        assert projection_expression == "#0"
        assert placeholders == {'Description': '#0'}


class ConditionExpressionTestCase(TestCase):

    def setUp(self):
        self.attribute = UnicodeAttribute(attr_name='foo')

    def test_equal(self):
        condition = self.attribute == 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 = :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}}

    def test_not_equal(self):
        condition = self.attribute != 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 <> :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}}

    def test_less_than(self):
        condition = self.attribute < 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 < :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}}

    def test_less_than_or_equal(self):
        condition = self.attribute <= 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 <= :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}}

    def test_greater_than(self):
        condition = self.attribute > 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 > :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}}

    def test_greater_than_or_equal(self):
        condition = self.attribute >= 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 >= :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}}

    def test_between(self):
        condition = self.attribute.between('bar', 'baz')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 BETWEEN :0 AND :1"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}}

    def test_in(self):
        condition = self.attribute.is_in('bar', 'baz')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 IN (:0, :1)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}}

    def test_exists(self):
        condition = self.attribute.exists()
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "attribute_exists (#0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {}

    def test_does_not_exist(self):
        condition = self.attribute.does_not_exist()
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "attribute_not_exists (#0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {}

    def test_is_type(self):
        condition = self.attribute.is_type()
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "attribute_type (#0, :0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S' : 'S'}}

    def test_begins_with(self):
        condition = self.attribute.startswith('bar')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "begins_with (#0, :0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S' : 'bar'}}

    def test_contains(self):
        condition = self.attribute.contains('bar')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "contains (#0, :0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S' : 'bar'}}

    def test_contains_string_set(self):
        condition = UnicodeSetAttribute(attr_name='foo').contains('bar')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "contains (#0, :0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S' : 'bar'}}

    def test_contains_number_set(self):
        condition = NumberSetAttribute(attr_name='foo').contains(1)
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "contains (#0, :0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'N' : '1'}}

    def test_contains_list(self):
        condition = ListAttribute(attr_name='foo').contains('bar')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "contains (#0, :0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S' : 'bar'}}

    def test_contains_attribute(self):
        condition = ListAttribute(attr_name='foo').contains(Path('bar'))
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "contains (#0, #1)"
        assert placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert expression_attribute_values == {}

    def test_size(self):
        condition = size(self.attribute) == 3
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "size (#0) = :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'N' : '3'}}

    def test_sizes(self):
        condition = size(self.attribute) == size(Path('bar'))
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "size (#0) = size (#1)"
        assert placeholder_names == {'foo': '#0', 'bar': '#1'}
        assert expression_attribute_values == {}

    def test_and(self):
        condition = (self.attribute < 'bar') & (self.attribute > 'baz')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "(#0 < :0 AND #0 > :1)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}}

    def test_or(self):
        condition = (self.attribute < 'bar') | (self.attribute > 'baz')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "(#0 < :0 OR #0 > :1)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}}

    def test_not(self):
        condition = ~(self.attribute < 'bar')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "(NOT #0 < :0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}}

    def test_compound_logic(self):
        condition = (~(self.attribute < 'bar') & (self.attribute > 'baz')) | (self.attribute == 'foo')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "(((NOT #0 < :0) AND #0 > :1) OR #0 = :2)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}, ':1': {'S': 'baz'}, ':2': {'S': 'foo'}}

    def test_indexing(self):
        condition = ListAttribute(attr_name='foo')[0] == 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0[0] = :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S' : 'bar'}}

    def test_invalid_indexing(self):
        with self.assertRaises(TypeError):
            self.attribute[0]

    def test_double_indexing(self):
        condition = ListAttribute(attr_name='foo')[0][1] == 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0[0][1] = :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S' : 'bar'}}

    def test_list_comparison(self):
        condition = ListAttribute(attr_name='foo') == ['bar', 'baz']
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 = :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'L': [{'S' : 'bar'}, {'S': 'baz'}]}}

    def test_dotted_attribute_name(self):
        self.attribute.attr_name = 'foo.bar'
        condition = self.attribute == 'baz'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 = :0"
        assert placeholder_names == {'foo.bar': '#0'}
        assert expression_attribute_values == {':0': {'S': 'baz'}}

    def test_map_attribute_dereference(self):
        class MyMapAttribute(MapAttribute):
            nested_string = self.attribute

        # Simulate initialization from inside an AttributeContainer
        my_map_attribute = MyMapAttribute(attr_name='foo.bar')
        my_map_attribute._make_attribute()
        my_map_attribute._update_attribute_paths(my_map_attribute.attr_name)

        condition = my_map_attribute.nested_string == 'baz'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0.#1 = :0"
        assert placeholder_names == {'foo.bar': '#0', 'foo': '#1'}
        assert expression_attribute_values == {':0': {'S': 'baz'}}


class UpdateExpressionTestCase(TestCase):

    def test_set_action(self):
        action = Path('foo').set('bar')
        placeholder_names, expression_attribute_values = {}, {}
        expression = action.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 = :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S': 'bar'}}

    def test_remove_action(self):
        action = Path('foo').remove()
        placeholder_names, expression_attribute_values = {}, {}
        expression = action.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {}

    def test_add_action(self):
        action = Path('foo').update(0)
        placeholder_names, expression_attribute_values = {}, {}
        expression = action.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'N': '0'}}

    def test_add_action_set(self):
        action = Path('foo').update({'NS': ['0']})
        placeholder_names, expression_attribute_values = {}, {}
        expression = action.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'NS': ['0']}}

    def test_add_action_list(self):
        with self.assertRaises(ValueError):
            Path('foo').update({'L': [{'N': '0'}]})

    def test_delete_action(self):
        action = Path('foo').difference_update({'NS': ['0']})
        placeholder_names, expression_attribute_values = {}, {}
        expression = action.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'NS': ['0']}}

    def test_delete_action_non_set(self):
        with self.assertRaises(ValueError):
            Path('foo').difference_update({'N': '0'})

    def test_update(self):
        path = Path('foo')
        update = Update()
        update.add_action(path.set({'S': 'bar'}))
        update.add_action(path.remove())
        update.add_action(path.update({'N': '0'}))
        update.add_action(path.difference_update({'NS': ['0']}))
        placeholder_names, expression_attribute_values = {}, {}
        expression = update.serialize(placeholder_names, expression_attribute_values)
        assert expression == "SET #0 = :0 REMOVE #0 ADD #0 :1 DELETE #0 :2"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {
            ':0': {'S': 'bar'},
            ':1': {'N': '0'},
            ':2': {'NS': ['0']}
        }

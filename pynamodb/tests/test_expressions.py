from pynamodb.attributes import UnicodeAttribute
from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.expressions.projection import create_projection_expression


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


class ConditionExpressionTestCase(TestCase):

    def setUp(self):
        self.attribute = UnicodeAttribute(attr_name='foo')

    def test_equals(self):
        condition = self.attribute == 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0 = :0"
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

    def test_begins_with(self):
        condition = self.attribute.startswith('bar')
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "begins_with (#0, :0)"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': {'S' : 'bar'}}

    def test_indexing(self):
        condition = self.attribute[0] == 'bar'
        placeholder_names, expression_attribute_values = {}, {}
        expression = condition.serialize(placeholder_names, expression_attribute_values)
        assert expression == "#0[0] = :0"
        assert placeholder_names == {'foo': '#0'}
        assert expression_attribute_values == {':0': 'bar'}  # TODO fix attribute value formatting

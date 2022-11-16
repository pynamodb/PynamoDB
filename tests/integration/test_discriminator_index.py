import pytest

import pynamodb.exceptions
from pynamodb.attributes import DiscriminatorAttribute
from pynamodb.attributes import DynamicMapAttribute
from pynamodb.attributes import ListAttribute
from pynamodb.attributes import MapAttribute
from pynamodb.attributes import NumberAttribute
from pynamodb.attributes import UnicodeAttribute
from pynamodb.indexes import AllProjection
from pynamodb.models import Model
from pynamodb.indexes import GlobalSecondaryIndex


class TestDiscriminatorIndex:

    def test_create_table(self, ddb_url):
        class ParentModel(Model, discriminator='Parent'):
            class Meta:
                host = ddb_url
                table_name = 'discriminator_index_test'
                read_capacity_units = 1
                write_capacity_units = 1

            hash_key = UnicodeAttribute(hash_key=True)
            cls = DiscriminatorAttribute()

        class ChildIndex(GlobalSecondaryIndex):
            class Meta:
                index_name = 'child_index'
                projection = AllProjection()
                read_capacity_units = 1
                write_capacity_units = 1

            index_key = UnicodeAttribute(hash_key=True)

        class ChildModel1(ParentModel, discriminator='Child1'):
            child_index = ChildIndex()
            index_key = UnicodeAttribute()

        # Multiple child models can share the same index
        class ChildModel2(ParentModel, discriminator='Child2'):
            child_index = ChildIndex()
            index_key = UnicodeAttribute()

        # What's important to notice is that the child_index is not defined on the parent class.
        # We're running `create_table` on the ParentModel, and expect it to know about child models
        # (through the discriminator association) and include all child models' indexes
        # during table creation.
        ParentModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

        model = ChildModel1()
        model.hash_key = 'hash_key1'
        model.index_key = 'bar'
        model.save()

        model = ChildModel2()
        model.hash_key = 'hash_key2'
        model.index_key = 'baz'
        model.save()

        model = next(ChildModel1.child_index.query('bar'))
        assert isinstance(model, ChildModel1)

        model = next(ChildModel2.child_index.query('baz'))
        assert isinstance(model, ChildModel2)

    def test_create_table__incompatible_indexes(self, ddb_url):
        class ParentModel(Model, discriminator='Parent'):
            class Meta:
                host = ddb_url
                table_name = 'discriminator_index_test__incompatible_indexes'
                read_capacity_units = 1
                write_capacity_units = 1

            hash_key = UnicodeAttribute(hash_key=True)
            cls = DiscriminatorAttribute()

        class ChildIndex1(GlobalSecondaryIndex):
            class Meta:
                index_name = 'child_index1'
                projection = AllProjection()
                read_capacity_units = 1
                write_capacity_units = 1

            index_key = UnicodeAttribute(hash_key=True)

        class ChildIndex2(GlobalSecondaryIndex):
            class Meta:
                index_name = 'child_index2'
                projection = AllProjection()
                read_capacity_units = 1
                write_capacity_units = 1

            # Intentionally a different type from ChildIndex1.index_key
            index_key = NumberAttribute(hash_key=True)

        # noinspection PyUnusedLocal
        class ChildModel1(ParentModel, discriminator='Child1'):
            child_index = ChildIndex1()
            index_key = UnicodeAttribute()

        # noinspection PyUnusedLocal
        class ChildModel2(ParentModel, discriminator='Child2'):
            child_index = ChildIndex2()
            index_key = UnicodeAttribute()

        # Unlike `test_create_table`, we expect this to fail because the child indexes
        # attempt to use the same attribute name for different types, thus the resulting table's
        # AttributeDefinitions would have the same attribute appear twice with conflicting types.
        with pytest.raises(pynamodb.exceptions.TableError, match="Cannot have two attributes with the same name"):
            ParentModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

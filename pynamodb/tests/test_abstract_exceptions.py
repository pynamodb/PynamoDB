"""
Test model API
"""
import base64
import random
import json
import copy
from datetime import datetime

import six
from botocore.client import ClientError
from botocore.vendored import requests
import pytest

from pynamodb.compat import CompatTestCase as TestCase
from pynamodb.tests.deep_eq import deep_eq
from pynamodb.connection.util import pythonic
from pynamodb.exceptions import DoesNotExist, TableError, NotAllowedWhenAbstract, InheritanceError
from pynamodb.types import RANGE
from pynamodb.constants import (
    ITEM, STRING_SHORT, ALL, KEYS_ONLY, INCLUDE, REQUEST_ITEMS, UNPROCESSED_KEYS, CAMEL_COUNT,
    RESPONSES, KEYS, ITEMS, LAST_EVALUATED_KEY, EXCLUSIVE_START_KEY, ATTRIBUTES, BINARY_SHORT,
    UNPROCESSED_ITEMS, DEFAULT_ENCODING, MAP_SHORT, LIST_SHORT, NUMBER_SHORT, SCANNED_COUNT
)
from pynamodb.models import Model, ResultSet
from pynamodb.indexes import (
    GlobalSecondaryIndex, LocalSecondaryIndex, AllProjection,
    IncludeProjection, KeysOnlyProjection, Index
)
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, BinaryAttribute, UTCDateTimeAttribute,
    UnicodeSetAttribute, NumberSetAttribute, BinarySetAttribute, MapAttribute,
    BooleanAttribute, ListAttribute)
from pynamodb.tests.data import (
    MODEL_TABLE_DATA, GET_MODEL_ITEM_DATA, SIMPLE_MODEL_TABLE_DATA,
    BATCH_GET_ITEMS, SIMPLE_BATCH_GET_ITEMS, COMPLEX_TABLE_DATA,
    COMPLEX_ITEM_DATA, INDEX_TABLE_DATA, LOCAL_INDEX_TABLE_DATA, DOG_TABLE_DATA,
    CUSTOM_ATTR_NAME_INDEX_TABLE_DATA, CUSTOM_ATTR_NAME_ITEM_DATA,
    BINARY_ATTR_DATA, SERIALIZED_TABLE_DATA, OFFICE_EMPLOYEE_MODEL_TABLE_DATA, COMPLEX_MODEL_SERIALIZED_TABLE_DATA,
    GET_OFFICE_EMPLOYEE_ITEM_DATA, GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL,
    GROCERY_LIST_MODEL_TABLE_DATA, GET_GROCERY_LIST_ITEM_DATA,
    GET_OFFICE_ITEM_DATA, OFFICE_MODEL_TABLE_DATA, COMPLEX_MODEL_TABLE_DATA, COMPLEX_MODEL_ITEM_DATA,
    CAR_MODEL_TABLE_DATA, FULL_CAR_MODEL_ITEM_DATA, CAR_MODEL_WITH_NULL_ITEM_DATA, INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA,
    BOOLEAN_CONVERSION_MODEL_TABLE_DATA,
    BOOLEAN_CONVERSION_MODEL_NEW_STYLE_FALSE_ITEM_DATA, BOOLEAN_CONVERSION_MODEL_NEW_STYLE_TRUE_ITEM_DATA,
    BOOLEAN_CONVERSION_MODEL_OLD_STYLE_FALSE_ITEM_DATA, BOOLEAN_CONVERSION_MODEL_OLD_STYLE_TRUE_ITEM_DATA,
    BOOLEAN_CONVERSION_MODEL_TABLE_DATA_OLD_STYLE, TREE_MODEL_TABLE_DATA, TREE_MODEL_ITEM_DATA,
    EXPLICIT_RAW_MAP_MODEL_TABLE_DATA, EXPLICIT_RAW_MAP_MODEL_ITEM_DATA,
    EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_ITEM_DATA, EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_TABLE_DATA
)

if six.PY3:
    from unittest.mock import patch, MagicMock
else:
    from mock import patch, MagicMock

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'

class Attrbuite3GlobalIndex(GlobalSecondaryIndex):
    class Meta:
        index_name = "AttrbuiteIndex1"
        read_capacity_units = 2
        write_capacity_units = 2
        projection = AllProjection()
    atrr3 = NumberAttribute(hash_key=True)

class Attrbuite4LocalIndex(LocalSecondaryIndex):
    class Meta:
        index_name = "AttrbuiteIndex2"
        read_capacity_units = 2
        write_capacity_units = 2
        projection = AllProjection()
    atrr4 = UnicodeAttribute(hash_key=True)

class AbstractModel0(Model):
    class Meta:
        _abstract_ = True
    atrr1 = UnicodeAttribute(hash_key=True)
    atrr2 = BinaryAttribute(null=True)
    atrr3 = NumberAttribute()
    atrr4 = UnicodeAttribute()


class AbstractModel1(Model):
    class Meta:
        _abstract_ = True
    atrr4 = UnicodeAttribute(hash_key=True)
    atrr5 = BinaryAttribute(null=True)

class AbstractModel2(AbstractModel0):
    pass


class ModelTestCase(TestCase):
    """
    Tests for the models API
    """
    @staticmethod
    def init_table_meta(model_clz, table_data):
        with patch(PATCH_METHOD) as req:
            req.return_value = table_data
            model_clz._get_meta_data()

    def assert_dict_lists_equal(self, list1, list2):
        """
        Compares two lists of dictionaries
        This function allows both the lists and dictionaries to have any order
        """
        if len(list1) != len(list2):
            raise AssertionError("Values not equal: {0} {1}".format(list1, list2))
        for d1_item in list1:
            found = False
            for d2_item in list2:
                if d2_item == d1_item:
                    found = True
            if not found:
                raise AssertionError("Values not equal: {0} {1}".format(list1, list2))

    def test_faulty_creation(self):
        with patch(PATCH_METHOD) as req:
            # Can't call create_table on abstract models
            with self.assertRaises(AbstractModel0.NotAllowedWhenAbstract):
                AbstractModel0.create_table(read_capacity_units=2, write_capacity_units=2)
            with self.assertRaises(AbstractModel1.NotAllowedWhenAbstract):
                AbstractModel1.create_table(read_capacity_units=2, write_capacity_units=2)
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.create_table(read_capacity_units=2, write_capacity_units=2)

            # Abstract models can't have GlobalSecondaryIndex
            with self.assertRaises(NotAllowedWhenAbstract):
                class AbstractModel3(AbstractModel0):
                    atrr3_index = Attrbuite3GlobalIndex()
            with self.assertRaises(NotAllowedWhenAbstract):
                class AbstractModel3(AbstractModel2):
                    atrr3_index = Attrbuite3GlobalIndex()
            with self.assertRaises(NotAllowedWhenAbstract):
                class AbstractModel3(Model):
                    class Meta:
                        _abstract_ = True
                    atrr1 = UnicodeAttribute(hash_key=True)
                    atrr2 = BinaryAttribute(null=True)
                    atrr3 = NumberAttribute()
                    atrr3_index = Attrbuite3GlobalIndex()

            # Abstract models can't have LocalSecondaryIndex
            with self.assertRaises(NotAllowedWhenAbstract):
                class AbstractModel3(AbstractModel0):
                    atrr4_index = Attrbuite4LocalIndex()
            with self.assertRaises(NotAllowedWhenAbstract):
                class AbstractModel3(AbstractModel2):
                    atrr4_index = Attrbuite4LocalIndex()
            with self.assertRaises(NotAllowedWhenAbstract):
                class AbstractModel3(Model):
                    class Meta:
                        _abstract_ = True
                    atrr1 = UnicodeAttribute(hash_key=True)
                    atrr2 = BinaryAttribute(null=True)
                    atrr4 = UnicodeAttribute()
                    atrr4_index = Attrbuite4LocalIndex()

            # Abstract models can't have a table_name
            with self.assertRaises(NotAllowedWhenAbstract):
                class AbstractModel3(Model):
                    class Meta:
                        _abstract_ = True
                        table_name = "AbstractTable3"
                    pass
            with self.assertRaises(NotAllowedWhenAbstract):
                class AbstractModel3(AbstractModel0):
                    class Meta:
                        table_name = "AbstractTable3"

            # Non Abstract models need a table_name
            #with self.assertRaises(InheritanceError):
            #    class NonAbstractModel0(Model):
            #        class Meta:
            #            _abstract_ = False
            #        pass
            #with self.assertRaises(InheritanceError):
            #    class NonAbstractModel0(AbstractModel0):
            #        class Meta:
            #            _abstract_ = False

            # Inheritance from multiple direct parents isn't allowed
            with self.assertRaises(InheritanceError):
                class NonAbstractModel0(AbstractModel0, AbstractModel1):
                    pass

            # Inheritance from non abstract parent isn't supported now
            with self.assertRaises(NotImplementedError):
                class NonAbstractModel0(AbstractModel0):
                    class Meta:
                        _abstract_ = False
                        table_name = "NonAbstractTable0"
                class NonAbstractModel1(NonAbstractModel0):
                    pass

    def test_not_allowed_class_methods(self):
        return
        with patch(PATCH_METHOD) as req:
            with self.assertRaises(AbstractModel0.NotAllowedWhenAbstract):
                AbstractModel0._check_not_abstract()
            with self.assertRaises(AbstractModel1.NotAllowedWhenAbstract):
                AbstractModel1._check_not_abstract()
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2._check_not_abstract()

            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_get({}, None, {})
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write()
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.get()
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.from_raw_data()
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write()
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write(True)
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write(True)
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write(True)
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write(True)
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write(True)
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write(True)
            with self.assertRaises(AbstractModel2.NotAllowedWhenAbstract):
                AbstractModel2.batch_write(True)


"""
Integration tests for the model API
"""
from __future__ import print_function
import pprint
from datetime import datetime
from pynamodb.connection import Connection
from pynamodb.models import Model
from pynamodb.constants import DATETIME_FORMAT
from pynamodb.attributes import (
    UnicodeAttribute, BinaryAttribute, UTCDateTimeAttribute, NumberSetAttribute
)


class TestModel(Model):
    """
    A model for testing
    """
    table_name = 'pynamodb-ci'
    forum = UnicodeAttribute(hash_key=True)
    thread = UnicodeAttribute(range_key=True)
    epoch = UTCDateTimeAttribute(default=datetime.now)
    content = BinaryAttribute(null=True)
    scores = NumberSetAttribute()

conn = Connection()
tables = conn.list_tables()
if TestModel.table_name not in tables['TableNames']:
    print("Creating table")
    TestModel.create_table(read_capacity_units=1, write_capacity_units=1)

pprint.pprint(TestModel.meta())
obj = TestModel('foo', 'bar')
print(obj.save())
obj2 = TestModel.get('foo', 'bar')
print(obj2)
print(obj.epoch.strftime(DATETIME_FORMAT), obj2.epoch.strftime(DATETIME_FORMAT))
obj3 = TestModel('setitem', 'setrange', scores={1, 2.1})
obj3.save()
print(obj3.scores)
obj3.update()
print(obj3.scores)
for item in TestModel.scan():
    print(item)

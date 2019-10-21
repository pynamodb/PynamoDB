"""
Integration tests for the model API
"""
from datetime import datetime
from pynamodb.models import Model
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection, LocalSecondaryIndex
from pynamodb.attributes import (
    UnicodeAttribute, BinaryAttribute, UTCDateTimeAttribute, NumberSetAttribute, NumberAttribute,
    VersionAttribute)

import pytest


class LSIndex(LocalSecondaryIndex):
    """
    A model for the local secondary index
    """
    class Meta:
        projection = AllProjection()
    forum = UnicodeAttribute(hash_key=True)
    view = NumberAttribute(range_key=True)


class GSIndex(GlobalSecondaryIndex):
    """
    A model for the secondary index
    """
    class Meta:
        projection = AllProjection()
        read_capacity_units = 2
        write_capacity_units = 1
    epoch = UTCDateTimeAttribute(hash_key=True)


@pytest.mark.ddblocal
def test_model_integration(ddb_url):

    class TestModel(Model):
        """
        A model for testing
        """
        class Meta:
            region = 'us-east-1'
            table_name = 'pynamodb-ci'
            host = ddb_url
        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        view = NumberAttribute(default=0)
        view_index = LSIndex()
        epoch_index = GSIndex()
        epoch = UTCDateTimeAttribute(default=datetime.now)
        content = BinaryAttribute(null=True)
        scores = NumberSetAttribute()
        version = VersionAttribute()

    if not TestModel.exists():
        print("Creating table")
        TestModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

    obj = TestModel('1', '2')
    obj.save()
    obj.refresh()
    obj = TestModel('foo', 'bar')
    obj.save()
    TestModel('foo2', 'bar2')
    obj3 = TestModel('setitem', 'setrange', scores={1, 2.1})
    obj3.save()
    obj3.refresh()

    with TestModel.batch_write() as batch:
        items = [TestModel('hash-{}'.format(x), '{}'.format(x)) for x in range(10)]
        for item in items:
            batch.save(item)

    item_keys = [('hash-{}'.format(x), 'thread-{}'.format(x)) for x in range(10)]

    for item in TestModel.batch_get(item_keys):
        print(item)

    for item in TestModel.query('setitem', TestModel.thread.startswith('set')):
        print("Query Item {}".format(item))

    with TestModel.batch_write() as batch:
        items = [TestModel('hash-{}'.format(x), '{}'.format(x)) for x in range(10)]
        for item in items:
            print("Batch delete")
            batch.delete(item)

    for item in TestModel.scan():
        print("Scanned item: {}".format(item))

    tstamp = datetime.now()
    query_obj = TestModel('query_forum', 'query_thread')
    query_obj.forum = 'foo'
    query_obj.save()
    query_obj.update([TestModel.view.add(1)])
    for item in TestModel.epoch_index.query(tstamp):
        print("Item queried from index: {}".format(item))

    for item in TestModel.view_index.query('foo', TestModel.view > 0):
        print("Item queried from index: {}".format(item.view))

    print(query_obj.update([TestModel.view.add(1)], condition=TestModel.forum.exists()))
    TestModel.delete_table()

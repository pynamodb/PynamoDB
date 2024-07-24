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
        content = BinaryAttribute(null=True, legacy_encoding=False)
        scores = NumberSetAttribute()
        version = VersionAttribute()

    if TestModel.exists():
        TestModel.delete_table()
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

    query_obj.update([TestModel.scores.set([])])
    query_obj.refresh()
    assert query_obj.scores is None

    print(query_obj.update([TestModel.view.add(1)], condition=TestModel.forum.exists()))
    TestModel.delete_table()


def test_can_inherit_version_attribute(ddb_url) -> None:

    class TestModelA(Model):
        """
        A model for testing
        """

        class Meta:
            region = 'us-east-1'
            table_name = 'pynamodb-ci-a'
            host = ddb_url

        forum = UnicodeAttribute(hash_key=True)
        thread = UnicodeAttribute(range_key=True)
        scores = NumberAttribute()
        version = VersionAttribute()

    class TestModelB(TestModelA):
        class Meta:
            region = 'us-east-1'
            table_name = 'pynamodb-ci-b'
            host = ddb_url

    with pytest.raises(ValueError) as e:
        class TestModelC(TestModelA):
            class Meta:
                region = 'us-east-1'
                table_name = 'pynamodb-ci-c'
                host = ddb_url

            version_invalid = VersionAttribute()
    assert str(e.value) == 'The model has more than one Version attribute: version, version_invalid'


@pytest.mark.ddblocal
def test_update_model_with_version_attribute_without_get(ddb_url):
    class TestModel(Model):
        """
        A model for testing
        """
        class Meta:
            region = 'us-east-1'
            table_name = 'pynamodb-ci'
            host = ddb_url
        forum = UnicodeAttribute(hash_key=True)
        scores = NumberSetAttribute()
        version = VersionAttribute()

    if TestModel.exists():
        TestModel.delete_table()
    TestModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

    obj = TestModel('1')
    obj.save()
    assert TestModel.get('1').version == 1
    obj.scores = 1 
    obj.save()
    assert TestModel.get('1').version == 2

    obj_by_key = TestModel('1')  # try to update item without getting it first
    obj_by_key.update(
        actions=[
            TestModel.scores.set(2),  # no version increment
        ],
        add_version_condition=False
    )
    updated_obj = TestModel.get('1')
    assert updated_obj.scores == 2
    assert updated_obj.version == 2


    obj_2 = TestModel('2')
    obj_2.save()
    assert TestModel.get('2').version == 1
    obj_2.scores = 1 
    obj_2.save()
    assert TestModel.get('2').version == 2

    obj_2_by_key = TestModel('2')  # try to update item without getting it first
    obj_2_by_key.update(
        actions=[
            TestModel.scores.set(2),
            TestModel.version.set(TestModel.version + 1)  # increment version manually
        ],
        add_version_condition=False
    )
    updated_obj_2 = TestModel.get('2')
    assert updated_obj_2.scores == 2
    assert updated_obj_2.version == 2

    TestModel.delete_table()

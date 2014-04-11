"""
An example using Amazon's Thread example for motivation

http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleTablesAndData.html
"""
from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute
)
from datetime import datetime


class Thread(Model):
    class Meta:
        table_name = "Thread"
        host = "http://localhost:8000"
    forum_name = UnicodeAttribute(hash_key=True)
    subject = UnicodeAttribute(range_key=True)
    views = NumberAttribute(default=0)
    replies = NumberAttribute(default=0)
    answered = NumberAttribute(default=0)
    tags = UnicodeSetAttribute()
    last_post_datetime = UTCDateTimeAttribute()

# Create the table
if not Thread.exists():
    Thread.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

# Create a thread
thread_item = Thread(
    'Some Forum',
    'Some Subject',
    tags=['foo', 'bar'],
    last_post_datetime=datetime.now()
)

# Save the thread
thread_item.save()

# Batch write operation
with Thread.batch_write() as batch:
    threads = []
    for x in range(100):
        thread = Thread('forum-{0}'.format(x), 'subject-{0}'.format(x))
        thread.tags = ['tag1', 'tag2']
        thread.last_post_datetime = datetime.now()
        threads.append(thread)

    for thread in threads:
        batch.save(thread)

# Batch get
item_keys = [('forum-{0}'.format(x), 'subject-{0}'.format(x)) for x in range(100)]
for item in Thread.batch_get(item_keys):
    print(item)

# Scan
for item in Thread.scan():
    print(item)

# Query
for item in Thread.query('forum-1', subject__begins_with='subject'):
    print(item)


print("-"*80)

# A model that uses aliased attribute names
class AliasedModel(Model):
    class Meta:
        table_name = "AliasedModel"
        host = "http://localhost:8000"
    forum_name = UnicodeAttribute(hash_key=True, attr_name='fn')
    subject = UnicodeAttribute(range_key=True, attr_name='s')
    views = NumberAttribute(default=0, attr_name='v')
    replies = NumberAttribute(default=0, attr_name='rp')
    answered = NumberAttribute(default=0, attr_name='an')
    tags = UnicodeSetAttribute(attr_name='t')
    last_post_datetime = UTCDateTimeAttribute(attr_name='lp')

if not AliasedModel.exists():
    AliasedModel.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

# Create a thread
thread_item = AliasedModel(
    'Some Forum',
    'Some Subject',
    tags=['foo', 'bar'],
    last_post_datetime=datetime.now()
)

# Save the thread
thread_item.save()

# Batch write operation
with AliasedModel.batch_write() as batch:
    threads = []
    for x in range(100):
        thread = AliasedModel('forum-{0}'.format(x), 'subject-{0}'.format(x))
        thread.tags = ['tag1', 'tag2']
        thread.last_post_datetime = datetime.now()
        threads.append(thread)

    for thread in threads:
        batch.save(thread)

# Batch get
item_keys = [('forum-{0}'.format(x), 'subject-{0}'.format(x)) for x in range(100)]
for item in AliasedModel.batch_get(item_keys):
    print(item)

# Scan
for item in AliasedModel.scan():
    print(item)

# Query
for item in AliasedModel.query('forum-1', s__begins_with='subject'):
    print(item)


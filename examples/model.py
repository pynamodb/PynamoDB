"""
An example using Amazon's Thread example for motivation

http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleTablesAndData.html
"""
from __future__ import print_function
import logging
from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute
)
from datetime import datetime

logging.basicConfig()
log = logging.getLogger("pynamodb")
log.setLevel(logging.DEBUG)
log.propagate = True


class Thread(Model):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "Thread"
        host = "http://localhost:8000"
    forum_name = UnicodeAttribute(hash_key=True)
    subject = UnicodeAttribute(range_key=True)
    views = NumberAttribute(default=0)
    replies = NumberAttribute(default=0)
    answered = NumberAttribute(default=0)
    tags = UnicodeSetAttribute()
    last_post_datetime = UTCDateTimeAttribute(null=True)

# Delete the table
# print(Thread.delete_table())

# Create the table
if not Thread.exists():
    Thread.create_table(wait=True)

# Create a thread
thread_item = Thread(
    'Some Forum',
    'Some Subject',
    tags=['foo', 'bar'],
    last_post_datetime=datetime.now()
)

# try:
#     Thread.get('does not', 'exist')
# except Thread.DoesNotExist:
#     pass

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

# Get table count
print(Thread.count())

# Count based on a filter
print(Thread.count('forum-1'))

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
    print("Batch get item: {0}".format(item))

# Scan
for item in AliasedModel.scan():
    print("Scanned item: {0}".format(item))

# Query
for item in AliasedModel.query('forum-1', subject__begins_with='subject'):
    print("Query using aliased attribute: {0}".format(item))

# Query on non key attributes
for item in Thread.query('forum-1', views__eq=0):
    print("Query result: {0}".format(item))

# Query with conditional operators
for item in Thread.query('forum-1', views__eq=0, replies__eq=0, conditional_operator='OR'):
    print("Query result: {0}".format(item))


# Scan with filters
for item in Thread.scan(subject__begins_with='subject', views__ge=0, conditional_operator='AND'):
    print("Scanned item: {0} {1}".format(item.subject, item.views))

# Scan with null filter
for item in Thread.scan(subject__begins_with='subject', last_post_datetime__null=True):
    print("Scanned item: {0} {1}".format(item.subject, item.views))

# Conditionally save an item
thread_item = Thread(
    'Some Forum',
    'Some Subject',
    tags=['foo', 'bar'],
    last_post_datetime=datetime.now()
)

# DynamoDB will only save the item if forum_name exists and is not null
print(thread_item.save(forum_name__null=False))

# DynamoDB will update the item, by adding 1 to the views attribute,
# if the forum_name attribute equals 'Some Forum' or the subject attribute is not null
print(thread_item.update_item(
    'views',
    1,
    action='add',
    conditional_operator='or',
    forum_name__eq='Some Forum',
    subject__null=False)
)

# DynamoDB will delete the item, only if the views attribute is equal to one
print(thread_item.delete(views__eq=1))


# Backup/restore example
# Print the size of the table
print("Table size: {}".format(Thread.describe_table().get('ItemCount')))

# Dump the entire table to a file
Thread.dump('thread.json')

# Optionally Delete all table items
# Commented out for safety
# for item in Thread.scan():
#     item.delete()
print("Table size: {}".format(Thread.describe_table().get('ItemCount')))

# Restore table from a file
Thread.load('thread.json')
print("Table size: {}".format(Thread.describe_table().get('ItemCount')))

# Dump the entire table to a string
serialized = Thread.dumps()

# Load the entire table from a string
Thread.loads(serialized)

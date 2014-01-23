"""
An example table using Amazon's Thread example

http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleTablesAndData.html
"""
from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute
)


class Thread(Model):
    table_name = 'Thread'
    forum_name = UnicodeAttribute(hash_key=True)
    subject = UnicodeAttribute(range_key=True)
    views = NumberAttribute(default=0)
    replies = NumberAttribute(default=0)
    answered = NumberAttribute(default=0)
    tags = UnicodeSetAttribute()
    last_post_datetime = UTCDateTimeAttribute()

Thread.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

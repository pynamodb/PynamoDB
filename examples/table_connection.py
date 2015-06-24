"""
Example use of the TableConnection API
"""
from pynamodb.connection import TableConnection

# Get a table connection
table = TableConnection('Thread', host='http://localhost:8000')

# If the table doesn't already exist, the rest of this example will not work.

# Describe the table
print(table.describe_table())

# Get an item
print(table.get_item('hash-key', 'range-key'))

# Put an item
table.put_item('hash-key', 'range-key', attributes={'forum_name': 'value'})

# Delete an item
table.delete_item('hash-key', 'range-key')

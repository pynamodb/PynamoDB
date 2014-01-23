"""
Example use of the TableConnection API
"""
from pynamodb.connection import TableConnection

# Get a tableection
table = TableConnection('table-name')

# Describe the table
print(table.describe_table())

# Get an item
print(table.get_item('hash-key', 'range-key'))

# Put an item
table.put_item('hash-key', 'range-key', attributes={'name': 'value'})

# Delete an item
table.delete_item('hash-key', 'range-key')
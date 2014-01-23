"""
Examples using a connection
"""
from __future__ import print_function
from pynamodb.connection import Connection

# Get a connection
conn = Connection()

# List tables
print(conn.list_tables())

# Describe a table
print(conn.describe_table('table-name'))

# Get an item
print(conn.get_item('table-name', 'hash-key', 'range-key'))

# Put an item
conn.put_item('table-name', 'hash-key', 'range-key', attributes={'name': 'value'})

# Delete an item
conn.delete_item('table-name', 'hash-key', 'range-key')

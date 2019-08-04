==================
Optimistic Locking
==================

Optimistic Locking is a strategy for ensuring that your database writes are not overwritten by the writes of others.
With optimistic locking, each item has an attribute that acts as a version number. If you retrieve an item from a
table, the application records the version number of that item. You can update the item, but only if the version number
on the server side has not changed. If there is a version mismatch, it means that someone else has modified the item
before you did. The update attempt fails, because you have a stale version of the item. If this happens, you simply
try again by retrieving the item and then trying to update it. Optimistic locking prevents you from accidentally
overwriting changes that were made by others. It also prevents others from accidentally overwriting your changes.

.. warning:: - Optimistic locking will not work properly if you use DynamoDB global tables as they use last-write-wins for concurrent updates.

See also:
`DynamoDBMapper Documentation on Optimistic Locking <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.OptimisticLocking.html>`_.

Version Attribute
-----------------

To enable optimistic locking for a table simply add a ``VersionAttribute`` to your model definition.

.. code-block:: python

  class OfficeEmployeeMap(MapAttribute):
      office_employee_id = UnicodeAttribute()
      person = UnicodeAttribute()

      def __eq__(self, other):
          return isinstance(other, OfficeEmployeeMap) and self.person == other.person

      def __repr__(self):
          return str(vars(self))


  class Office(Model):
      class Meta:
          read_capacity_units = 1
          write_capacity_units = 1
          table_name = 'Office'
          host = "http://localhost:8000"
      office_id = UnicodeAttribute(hash_key=True)
      employees = ListAttribute(of=OfficeEmployeeMap)
      name = UnicodeAttribute()
      version = VersionAttribute()

The attribute is underpinned by an integer which is initialized with 1 when an item is saved for the first time
and is incremented by 1 with each subsequent write operation.

.. code-block:: python

  justin = OfficeEmployeeMap(office_employee_id=str(uuid4()), person='justin')
  garrett = OfficeEmployeeMap(office_employee_id=str(uuid4()), person='garrett')
  office = Office(office_id=str(uuid4()), name="office", employees=[justin, garrett])
  office.save()
  assert office.version == 1

  # Get a second local copy of Office
  office_out_of_date = Office.get(office.office_id)

  # Add another employee and persist the change.
  office.employees.append(OfficeEmployeeMap(office_employee_id=str(uuid4()), person='lita'))
  office.save()
  # On subsequent save or update operations the version is also incremented locally to match the persisted value so
  # there's no need to refresh between operations when reusing the local copy.
  assert office.version == 2
  assert office_out_of_date.version == 1

The version checking is implemented using DynamoDB conditional write constraints, asserting that no value exists
for the version attribute on the initial save and that the persisted value matches the local value on subsequent writes.


Model.{update, save, delete}
----------------------------
These operations will fail if the local object is out-of-date.

.. code-block:: python

  @contextmanager
  def assert_condition_check_fails():
      try:
          yield
      except (PutError, UpdateError, DeleteError) as e:
          assert isinstance(e.cause, ClientError)
          assert e.cause_response_code == "ConditionalCheckFailedException"
      except TransactWriteError as e:
          assert isinstance(e.cause, ClientError)
          assert e.cause_response_code == "TransactionCanceledException"
          assert "ConditionalCheckFailed" in e.cause_response_message
      else:
          raise AssertionError("The version attribute conditional check should have failed.")


  with assert_condition_check_fails():
      office_out_of_date.update(actions=[Office.name.set('new office name')])

  office_out_of_date.employees.remove(garrett)
  with assert_condition_check_fails():
      office_out_of_date.save()

  # After refreshing the local copy our write operations succeed.
  office_out_of_date.refresh()
  office_out_of_date.employees.remove(garrett)
  office_out_of_date.save()
  assert office_out_of_date.version == 3

  with assert_condition_check_fails():
      office.delete()

Transactions
------------

Transactions are supported.

Successful
__________

.. code-block:: python

  connection = Connection(host='http://localhost:8000')

  office2 = Office(office_id=str(uuid4()), name="second office", employees=[justin])
  office2.save()
  assert office2.version == 1
  office3 = Office(office_id=str(uuid4()), name="third office", employees=[garrett])
  office3.save()
  assert office3.version == 1

  with TransactWrite(connection=connection) as transaction:
      transaction.condition_check(Office, office.office_id, condition=(Office.name.exists()))
      transaction.delete(office2)
      transaction.save(Office(office_id=str(uuid4()), name="new office", employees=[justin, garrett]))
      transaction.update(
          office3,
          actions=[
              Office.name.set('birdistheword'),
          ]
      )

  try:
      office2.refresh()
  except DoesNotExist:
      pass
  else:
      raise AssertionError(
          'Office with office_id="{}" should have been deleted in the transaction.'
          .format(office2.office_id)
      )

  assert office.version == 2
  assert office3.version == 2

Failed
______

.. code-block:: python

  with assert_condition_check_fails(), TransactWrite(connection=connection) as transaction:
      transaction.save(Office(office.office_id, name='newer name', employees=[]))

  with assert_condition_check_fails(), TransactWrite(connection=connection) as transaction:
      transaction.update(
          Office(office.office_id, name='newer name', employees=[]),
          actions=[Office.name.set('Newer Office Name')]
      )

  with assert_condition_check_fails(), TransactWrite(connection=connection) as transaction:
      transaction.delete(Office(office.office_id, name='newer name', employees=[]))

Batch Operations
----------------
*Unsupported* as they do not support conditional writes.

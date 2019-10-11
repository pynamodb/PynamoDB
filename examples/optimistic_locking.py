from contextlib import contextmanager
from uuid import uuid4
from botocore.client import ClientError

from pynamodb.connection import Connection
from pynamodb.attributes import ListAttribute, MapAttribute, UnicodeAttribute, VersionAttribute
from pynamodb.exceptions import PutError, UpdateError, TransactWriteError, DeleteError, DoesNotExist
from pynamodb.models import Model
from pynamodb.transactions import TransactWrite


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


if not Office.exists():
    Office.create_table(wait=True)


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


justin = OfficeEmployeeMap(office_employee_id=str(uuid4()), person='justin')
garrett = OfficeEmployeeMap(office_employee_id=str(uuid4()), person='garrett')
office = Office(office_id=str(uuid4()), name="office 3", employees=[justin, garrett])
office.save()
assert office.version == 1

# Get a second local copy of Office
office_out_of_date = Office.get(office.office_id)
# Add another employee and save the changes.
office.employees.append(OfficeEmployeeMap(office_employee_id=str(uuid4()), person='lita'))
office.save()
# After a successful save or update operation the version is set or incremented locally so there's no need to refresh
# between operations using the same local copy.
assert office.version == 2
assert office_out_of_date.version == 1

# Condition check fails for update.
with assert_condition_check_fails():
    office_out_of_date.update(actions=[Office.name.set('new office name')])

# Condition check fails for save.
office_out_of_date.employees.remove(garrett)
with assert_condition_check_fails():
    office_out_of_date.save()

# After refreshing the local copy the operation will succeed.
office_out_of_date.refresh()
office_out_of_date.employees.remove(garrett)
office_out_of_date.save()
assert office_out_of_date.version == 3

# Condition check fails for delete.
with assert_condition_check_fails():
    office.delete()

# Example failed transactions.
connection = Connection(host='http://localhost:8000')

with assert_condition_check_fails(), TransactWrite(connection=connection) as transaction:
    transaction.save(Office(office.office_id, name='newer name', employees=[]))

with assert_condition_check_fails(), TransactWrite(connection=connection) as transaction:
    transaction.update(
        Office(office.office_id, name='newer name', employees=[]),
        actions=[
            Office.name.set('Newer Office Name'),
        ]
    )

with assert_condition_check_fails(), TransactWrite(connection=connection) as transaction:
    transaction.delete(Office(office.office_id, name='newer name', employees=[]))

# Example successful transaction.
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
        "This item should have been deleted, but no DoesNotExist "
        "exception was raised when attempting to refresh a local copy."
    )

assert office.version == 2
# The version attribute of items which are saved or updated in a transaction are updated automatically to match the
# persisted value.
assert office3.version == 2
office.refresh()
assert office.version == 3

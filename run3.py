from pynamodb.attributes import (
    UnicodeAttribute, UnicodeSetAttribute, Attribute, LegacyBooleanAttribute,
    BooleanAttribute, NumberAttribute, BinaryAttribute,MapAttribute, ListAttribute)

from pynamodb.models import Model

class MyModel(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    id = UnicodeAttribute(hash_key=True)
    name = UnicodeAttribute(range_key=True)

if MyModel.exists():
    MyModel.delete_table()
MyModel.create_table(read_capacity_units=2, write_capacity_units=2)



MyModel("1", "a1").save()
MyModel("1", "ab1").save()
MyModel("1", "c1").save()
MyModel("2", "b2").save()

for user in MyModel.scan(rate_limit=5):
    print("User id: {}, name: {}".format(user.id, user.name))

for user in MyModel.query('1', MyModel.name.startswith('a'), rate_limit = 15):
    print("Query returned user {0}".format(user))

print(MyModel.count("1", filter_condition= MyModel.name.startswith('a'), rate_limit=2))
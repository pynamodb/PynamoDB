from pynamodb.attributes import (
    UnicodeAttribute, UnicodeSetAttribute, Attribute, LegacyBooleanAttribute,
    BooleanAttribute, NumberAttribute, BinaryAttribute,MapAttribute, ListAttribute, EnumAttribute)

from pynamodb.models import Model

class MyModel(Model):
    class Meta:
        table_name = "TestModel"
        host="http://localhost:8000"
    id = UnicodeAttribute(hash_key=True)
    enum_attr = EnumAttribute(type_set={UnicodeAttribute(null=True), NumberAttribute(null=True), UnicodeSetAttribute(null=True)}, null=True)

if MyModel.exists():
    MyModel.delete_table()
MyModel.create_table(read_capacity_units=2, write_capacity_units=2)


############# Basic Usage ###############
# Create an object
m1 = MyModel("id1")
m1.enum_attr['UnicodeAttribute'] = "Obama"
m1.save()
print (MyModel.dumps())
# Outputs:
# [["id1", {"attributes": {"enum_attr": {"S": "{\"S\": \"Obama\"}"}}}]]
########################################

# Read an object
m1 = MyModel.get("id1")
print (m1.enum_attr.get_value())
# Outputs:
# Obama
########################################

# Change value
m1 = MyModel.get("id1")
m1.enum_attr["UnicodeAttribute"] = "Obama"
print (m1.enum_attr["UnicodeAttribute"])
# Outputs:
# Obama
m1.enum_attr["NumberAttribute"] = 2018
print (m1.enum_attr["NumberAttribute"])
# Outputs:
# 2018
print (m1.enum_attr["UnicodeAttribute"])
# Outputs:
# None
m1.save()
print (MyModel.dumps())
# Outputs:
# [["id1", {"attributes": {"enum_attr": {"S": "{\"N\": \"2018\"}"}}}]]
########################################

# Different assignment patterns
m2 = MyModel("id2")
# All have the same effect
m2.enum_attr["UnicodeAttribute"] = "President"
m2.enum_attr["S"] = "President"
m2.enum_attr["String"] = "President"
m2.save()
print (MyModel.dumps())
# Outputs:
# [["id1", {"attributes": {"enum_attr": {"S": "{\"N\": \"2018\"}"}}}], ["id2", {"attributes": {"enum_attr": {"S": "{\"S\": \"President\"}"}}}]] #noqa
########################################

# Nested values
m3 = MyModel("id3")
m3.enum_attr[UnicodeSetAttribute]={"Hi", "there", "World!"}
m3.save()
m3 = MyModel.get("id3")
print (m3.enum_attr.get_value())
# Outputs:
#set([u'there', u'Hi', u'World!'])
########################################

# Querying
for x in MyModel.query("id1", filter_condition=MyModel.enum_attr==(NumberAttribute,2018)):
    print ("id: {}, enum_attr: {}".format(x.id, x.enum_attr.get_value()))
# Outputs:
#id: id1, enum_attr: 2018

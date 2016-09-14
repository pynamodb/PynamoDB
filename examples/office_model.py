from pynamodb.attributes import ListAttribute, MapAttribute, NumberAttribute, UnicodeAttribute
from pynamodb.models import Model


class Location(MapAttribute):

    lat = NumberAttribute(attr_name='latitude')
    lng = NumberAttribute(attr_name='longitude')
    name = UnicodeAttribute()


class Person(MapAttribute):

    fname = UnicodeAttribute(attr_name='firstName')
    lname = UnicodeAttribute()
    age = NumberAttribute()


class OfficeEmployeeMap(MapAttribute):

    office_employee_id = NumberAttribute()
    person = Person()
    office_location = Location()


class Office(Model):
    class Meta:
        table_name = 'OfficeModel'
        host = "http://localhost:8000"

    office_id = NumberAttribute(hash_key=True)
    address = Location()
    employees = ListAttribute(of=OfficeEmployeeMap)

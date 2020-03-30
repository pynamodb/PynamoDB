Update Operations
=================

The UpdateItem DynamoDB operations allows you to create or modify attributes of an item using an update expression.
See the `official documentation <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html>`_
for more details.

Suppose that you have defined a `Thread` Model for the examples below.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import (
        ListAttribute, UnicodeAttribute, UnicodeSetAttribute, NumberAttribute
    )


    class Thread(Model):
        class Meta:
            table_name = 'Thread'

        forum_name = UnicodeAttribute(hash_key=True)
        subjects = UnicodeSetAttribute(default=dict)
        views = NumberAttribute(default=0)
        notes = ListAttribute(default=list)


.. _updates:

Update Expressions
^^^^^^^^^^^^^^^^^^

PynamoDB supports creating update expressions from attributes using a mix of built-in operators and method calls.
Any value provided will be serialized using the serializer defined for that attribute.

.. csv-table::
    :header: DynamoDB Action / Operator, PynamoDB Syntax, Example

    SET, set( `value` ), Thread.views.set(10)
    REMOVE, remove(), Thread.subjects.remove()
    ADD, add( `value` ), "Thread.subjects.add({'A New Subject', 'Another New Subject'})"
    DELETE, delete( `value` ), Thread.subjects.delete({'An Old Subject'})
    `attr_or_value_1` \+ `attr_or_value_2`, `attr_or_value_1` \+ `attr_or_value_2`, Thread.views + 5
    `attr_or_value_1` \- `attr_or_value_2`, `attr_or_value_1` \- `attr_or_value_2`, 5 - Thread.views
    "list_append( `attr` , `value` )", append( `value` ), Thread.notes.append(['my last note'])
    "list_append( `value` , `attr` )", prepend( `value` ), Thread.notes.prepend(['my first note'])
    "REMOVE list[index1], list[index2]", "remove_indexes(`index1`, `index2`)", "Thread.notes.remove_indexes(0, 1)"
    "if_not_exists( `attr`, `value` )", `attr` | `value`, Thread.forum_name | 'Default Forum Name'

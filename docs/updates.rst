Update Operations
=================

The UpdateItem DynamoDB operations allows you to create or modify attributes of an item using an update expression.
See the `official documentation <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html>`_
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
        subjects = UnicodeSetAttribute(default=set)
        author = UnicodeAttribute(null=True)
        views = NumberAttribute(default=0)
        notes = ListAttribute(default=list)


.. _updates:

Update Expressions
^^^^^^^^^^^^^^^^^^

PynamoDB supports creating update expressions from attributes using a mix of built-in operators and method calls.
Any value provided will be serialized using the serializer defined for that attribute.

.. csv-table::
    :header: DynamoDB Action / Operator, PynamoDB Syntax, Attribute Types, Example

    SET, set( `value` ), Any, :code:`Thread.views.set(10)`
    REMOVE, remove(), "Any", :code:`Thread.notes.remove()`
    REMOVE, remove(), "Element of List", :code:`Thread.notes[0].remove()`
    ADD, add( `number` ), "Number", ":code:`Thread.views.add(1)`"
    ADD, add( `set` ), "Set", ":code:`Thread.subjects.add({'A New Subject', 'Another New Subject'})`"
    DELETE, delete( `set` ), "Set", :code:`Thread.subjects.delete({'An Old Subject'})`

The following expressions and functions can only be used in the context of the above actions:

.. csv-table::
    :header: DynamoDB Action / Operator, PynamoDB Syntax, Attribute Types, Example

    `attr_or_value_1` \+ `attr_or_value_2`, `attr_or_value_1` \+ `attr_or_value_2`, "Number", :code:`Thread.views + 5`
    `attr_or_value_1` \- `attr_or_value_2`, `attr_or_value_1` \- `attr_or_value_2`, "Number", :code:`5 - Thread.views`
    "list_append( `attr` , `value` )", append( `value` ), "List", :code:`Thread.notes.append(['my last note'])`
    "list_append( `value` , `attr` )", prepend( `value` ), "List", :code:`Thread.notes.prepend(['my first note'])`
    "if_not_exists( `attr`, `value` )", `attr` | `value`, Any, :code:`Thread.forum_name | 'Default Forum Name'`

``set`` action
""""""""""""""

The ``set`` action is the simplest action as it overwrites any previously stored value:

.. code-block:: python

    thread.update(actions=[
        Thread.views.set(10),
    ])
    assert thread.views == 10

It can reference existing values (from this or other attributes) for arithmetics and concatenation:

.. code-block:: python

    # Increment views by 5
    thread.update(actions=[
        Thread.views.set(Thread.views + 5)
    ])

    # Append 2 notes
    thread.update(actions=[
        Thread.notes.set(
            Thread.notes.append([
                'my last note',
                'p.s. no, really, this is my last note',
            ]),
        )
    ])

    # Prepend a note
    thread.update(actions=[
        Thread.notes.set(
            Thread.notes.prepend([
                'my first note',
            ]),
        )
    ])

    # Set author to John Doe unless there's already one
    thread.update(actions=[
        Thread.author.set(Thread.author | 'John Doe')
    ])

``remove`` action
^^^^^^^^^^^^^^^^^

The ``remove`` action unsets attributes:

.. code-block:: python

    thread.update(actions=[
        Thread.views.remove(),
    ])
    assert thread.views == 0  # default value

It can also be used to remove elements from a list attribute:

.. code-block:: python

    # Remove the first note
    thread.update(actions=[
        Thread.notes[0].remove(),
    ])


``add`` action
^^^^^^^^^^^^^^

Applying to (binary, number and string) set attributes, the ``add`` action adds elements to the set:

.. code-block:: python

    # Add the subjects 'A New Subject' and 'Another New Subject'
    thread.update(actions=[
        Thread.subjects.add({'A New Subject', 'Another New Subject'})
    ])

Applying to number attributes, the ``add`` action increments or decrements the number
and is equivalent to a ``set`` action:

.. code-block:: python

    # Increment views by 5
    thread.update(actions=[
        Thread.views.add(5),
    ])
    # Also increment views by 5
    thread.update(actions=[
        Thread.views.set(Thread.views + 5),
    ])

``delete`` action
^^^^^^^^^^^^^^^^^

For set attributes, the ``delete`` action is the opposite of the ``add`` action:

.. code-block:: python

    # Delete the subject 'An Old Subject'
    thread.update(actions=[
        Thread.subjects.delete({'An Old Subject'})
    ])

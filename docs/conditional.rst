Conditional Operations
======================

Some DynamoDB operations (UpdateItem, PutItem, DeleteItem) support the inclusion of conditions. The user can supply a condition to be
evaluated by DynamoDB before the operation is performed. See the `official documentation <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate>`_
for more details.

Suppose that you have defined a `Thread` Model for the examples below.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import (
        UnicodeAttribute, NumberAttribute
    )


    class Thread(Model):
        class Meta:
            table_name = 'Thread'

        forum_name = UnicodeAttribute(hash_key=True)
        subject = UnicodeAttribute(range_key=True)
        views = NumberAttribute(default=0)

.. _conditions:

Condition Expressions
^^^^^^^^^^^^^^^^^^^^^

PynamoDB supports creating condition expressions from attributes using a mix of built-in operators and method calls.
Any value provided will be serialized using the serializer defined for that attribute.
See the `comparison operator and function reference <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html>`_
for more details.

.. csv-table::
    :header: DynamoDB Condition, PynamoDB Syntax, Example

    =, ==, Thread.forum_name == 'Some Forum'
    <>, !=, Thread.forum_name != 'Some Forum'
    <, <, Thread.views < 10
    <=, <=, Thread.views <= 10
    >, >, Thread.views > 10
    >=, >=, Thread.views >= 10
    BETWEEN, "between( `lower` , `upper` )", "Thread.views.between(1, 5)"
    IN, is_in( `*values` ), "Thread.subject.is_in('Subject', 'Other Subject')"
    attribute_exists ( `path` ), exists(), Thread.forum_name.exists()
    attribute_not_exists ( `path` ), does_not_exist(), Thread.forum_name.does_not_exist()
    "attribute_type ( `path` , `type` )", is_type(), Thread.forum_name.is_type()
    "begins_with ( `path` , `substr` )", startswith( `prefix` ), Thread.subject.startswith('Example')
    "contains ( `path` , `operand` )", contains( `item` ), Thread.subject.contains('foobar')
    size ( `path`), size( `attribute` ), size(Thread.subject) == 10
    AND, &, (Thread.views > 1) & (Thread.views < 5)
    OR, \|, (Thread.views < 1) | (Thread.views > 5)
    NOT, ~, ~Thread.subject.contains('foobar')

Conditions expressions using nested list and map attributes can be created with Python's item operator ``[]``:

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import (
        ListAttribute, MapAttribute, UnicodeAttribute
    )

    class Container(Model):
        class Meta:
            table_name = 'Container'

        name = UnicodeAttribute(hash_key = True)
        my_map = MapAttribute()
        my_list = ListAttribute()

    print(Container.my_map['foo'].exists() | Container.my_list[0].contains('bar'))


Conditional Model.save
^^^^^^^^^^^^^^^^^^^^^^

This example saves a `Thread` item, only if the item exists.

.. code-block:: python

    thread_item = Thread('Existing Forum', 'Example Subject')

    # DynamoDB will only save the item if forum_name exists
    print(thread_item.save(Thread.forum_name.exists())

    # You can specify multiple conditions
    print(thread_item.save(Thread.forum_name.exists() & Thread.forum_subject.contains('foobar')))


Conditional Model.update
^^^^^^^^^^^^^^^^^^^^^^^^

This example will update a `Thread` item, if the `views` attribute is less than 5 *OR* greater than 10:

.. code-block:: python

    thread_item.update(condition=(Thread.views < 5) | (Thread.views > 10))


Conditional Model.delete
^^^^^^^^^^^^^^^^^^^^^^^^

This example will delete the item, only if its `views` attribute is equal to 0.

.. code-block:: python

    print(thread_item.delete(Thread.views == 0))

Conditional Operation Failures
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can check for conditional operation failures by inspecting the cause of the raised exception:

.. code-block:: python

    try:
        thread_item.save(Thread.forum_name.exists())
    except PutError as e:
        if isinstance(e.cause, ClientError):
            code = e.cause.response['Error'].get('Code')
            print(code == "ConditionalCheckFailedException")

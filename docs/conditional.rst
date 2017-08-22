Conditional Operations
======================

Some DynamoDB operations (UpdateItem, PutItem, DeleteItem) support the inclusion of conditions. The user can supply a condition to be
evaluated by DynamoDB before the operation is performed. See the `official documentation <http://http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate>`_
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


Condition Expressions
^^^^^^^^^^^^^^^^^^^^^

PynamoDB supports creating condition expressions from attributes using a simple syntax. Attributes can be compared against values.
Any value provided will be serialized using the serializer defined for that attribute.

.. code-block:: python

    print(Thread.forum_name == 'Some Forum')
    print(Thread.views.between(1, 5))
    print(Thread.subject.is_in('Example Subject', 'Other Example Subject'))

The supported comparisons are:

 * ==
 * !=
 * <
 * <=
 * >
 * >=
 * between
 * is_in

Attributes can also be checked for existence or evaluated:

.. code-block:: python

    print(Thread.forum_name.exists())
    print(Thread.subject.startswith('Example'))
    print(Thread.subject.contains('foobar'))

The supported functions are:

 * exists
 * does_not_exist
 * is_type
 * startswith
 * contains
 * size()

Conditions can be combined using logical operations:

.. code-block:: python

    print(Thread.forum_name.does_not_exist() | (Thread.views.between(1, 5) & ~(Thread.views == 3)))

Finally, if necessary, you can use document paths to access nested list and map attributes:

.. code-block:: python

    from pynamodb.expressions.condition import size

    print(size('foo.bar[0].baz') == 0)


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

    thread_item.update((Thread.views < 5) | (Thread.views > 10))


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

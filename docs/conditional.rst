.. _conditional_operations:

Conditional Operations
======================

Some DynamoDB operations (UpdateItem, PutItem, DeleteItem) support the inclusion of conditions. The user can supply a condition to be
evaluated by DynamoDB before the operation is performed. See the `official documentation <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate>`_
for more details.

Suppose that you have defined a `Thread` Model for the examples below.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute, NumberAttribute


    class Thread(Model):
        class Meta:
            table_name = 'Thread'

        forum_name = UnicodeAttribute(hash_key=True)
        subject = UnicodeAttribute(range_key=True)
        views = NumberAttribute(default=0)
        authors = ListAttribute()
        properties = MapAttribute()


.. _conditions:

Condition Expressions
^^^^^^^^^^^^^^^^^^^^^

PynamoDB supports creating condition expressions from attributes using a mix of built-in operators and method calls.
Any value provided will be serialized using the serializer defined for that attribute.
See the `comparison operator and function reference <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html>`_
for more details.

.. csv-table::
    :header: DynamoDB Condition, PynamoDB Syntax, Attribute Types, Example

    =, ==, Any, :code:`Thread.forum_name == 'Some Forum'`
    <>, !=, Any, :code:`Thread.forum_name != 'Some Forum'`
    <, <, Any, :code:`Thread.views < 10`
    <=, <=, Any, :code:`Thread.views <= 10`
    >, >, Any, :code:`Thread.views > 10`
    >=, >=, Any, :code:`Thread.views >= 10`
    BETWEEN, "between( `lower` , `upper` )", "TODO", ":code:`Thread.views.between(1, 5)`"
    IN, is_in( `*values` ), Any, ":code:`Thread.subject.is_in('Subject', 'Other Subject')`"
    attribute_exists ( `path` ), exists(), Any, :code:`Thread.forum_name.exists()`
    attribute_not_exists ( `path` ), does_not_exist(), Any, :code:`Thread.forum_name.does_not_exist()`
    "attribute_type ( `path` , `type` )", is_type(), Any, :code:`Thread.forum_name.is_type()`
    "begins_with ( `path` , `substr` )", startswith( `prefix` ), String, :code:`Thread.subject.startswith('Example')`
    "contains ( `path` , `operand` )", contains( `item` ), "Set, String", :code:`Thread.subject.contains('foobar')`
    size ( `path`), size( `attribute` ), "Binary, List, Map, Set, String", :code:`size(Thread.subject) == 10`
    AND, &, Any, :code:`(Thread.views > 1) & (Thread.views < 5)`
    OR, \|, Any, :code:`(Thread.views < 1) | (Thread.views > 5)`
    NOT, ~, Any, :code:`~Thread.subject.contains('foobar')`

Conditions expressions using nested list and map attributes can be created with Python's item operator ``[]``.

.. code-block:: python

    # the 'properties' map contains key 'emoji'
    Thread.properties['emoji'].exists()

    # the first author's name contains "John"
    Thread.authors[0].contains("John")

Conditions can be composited using ``&`` (AND) and ``|`` (OR) operators. For the ``&`` (AND) operator, the left-hand side
operand can be ``None`` to allow easier chaining of filter conditions:

.. code-block:: python

  condition = None

  if request.subject:
    condition &= Thread.subject.contains(request.subject)

  if request.min_views:
    condition &= Thread.views >= min_views

  results = Thread.query(..., filter_condition=condition)

Conditioning on keys
^^^^^^^^^^^^^^^^^^^^

An ``exists()`` condition on a key ensures that the item already exists (under
the given key) in the table at the time the operation is performed. For example,
a `save` or `update` would update an existing item but fail if the item does not exist.

Correspondingly, a ``does_not_exist()`` condition on a key ensures that the item
does not exist. For example, a `save` with such a condition ensures that it's not
overwriting an existing item.

For models with a range key, conditioning ``exists()`` on either the hash key
or the range key has the same effect. There is no way to condition on _some_ item
existing with the given hash key. For example:

.. code-block:: python

    thread = Thread('DynamoDB', 'Using conditions')

    # This will fail if the item ('DynamoDB', 'Using conditions') does not exist,
    # even if the item ('DynamoDB', 'Using update expressions') does.
    thread.save(condition=Thread.forum_name.exists())

    # This will fail if the item ('DynamoDB', 'Using conditions') does not exist,
    # even if the item ('S3', 'Using conditions') does.
    thread.save(condition=Thread.subject.exists())


Conditional Model.save
^^^^^^^^^^^^^^^^^^^^^^

This example saves a `Thread` item, only if the item exists.

.. code-block:: python

    thread_item = Thread('Existing Forum', 'Example Subject')

    # DynamoDB will only save the item if forum_name exists
    print(thread_item.save(Thread.forum_name.exists())

    # You can specify multiple conditions
    print(thread_item.save(Thread.forum_name.exists() & Thread.subject.contains('foobar')))


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
        if e.cause_response_code = "ConditionalCheckFailedException":
            raise ThreadDidNotExistError()

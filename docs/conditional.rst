Conditional Operations
======================

Some DynamoDB operations (UpdateItem, PutItem, DeleteItem) support the inclusion of conditions. The user can supply a list of conditions to be
evaluated by DynamoDB before the operation is performed, as well as specifying whether those conditions are
applied with logical OR (at least one must be true) or logical AND (all must be true). See the `official documentation <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#ConditionalExpressions>`_
for more details. PynamoDB supports conditionals through keyword arguments, using syntax that is similar to the filter syntax (see :ref:`filtering`).
Multiple conditions may be supplied, and each value provided will be serialized using the serializer defined for that attribute.

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


AND vs. OR
^^^^^^^^^^

Specifying that the conditions should be applied with AND or OR is achieved through the use of the `conditional_operator` keyword,
which can be `and` or `or`.

.. code-block:: python

    thread_item = Thread('Existing Forum', 'Example Subject')

    # The item will be saved if the forum name is not null OR the subject contains 'foobar'
    thread_item.save(
        forum_name__null=False,
        forum_subject__contains='foobar',
        conditional_operator='or'
    )

    # The item will be saved if the forum name is not null AND the subject contains 'foobar'
    thread_item.save(
        forum_name__null=False,
        forum_subject__contains='foobar',
        conditional_operator='and'
    )

Conditional Model.save
^^^^^^^^^^^^^^^^^^^^^^

The following conditional operators are supported for `Model.save`:

* eq
* ne
* le
* lt
* ge
* gt
* null
* contains
* not_contains
* begins_with
* in
* between

This example saves a `Thread` item, only if the item exists, and the `forum_name` attribute is not null.

.. code-block:: python

    thread_item = Thread('Existing Forum', 'Example Subject')

    # DynamoDB will only save the item if forum_name exists and is not null
    print(thread_item.save(forum_name__null=False)

    # You can specify multiple conditions
    print(thread_item.save(forum_name__null=False, forum_subject__contains='foobar'))


Conditional Model.update
^^^^^^^^^^^^^^^^^^^^^^^^

The following conditional operators are supported for `Model.update`:

* eq
* ne
* le
* lt
* ge
* gt
* null
* contains
* not_contains
* begins_with
* in
* between

This example will update a `Thread` item, if the `forum_name` attribute equals 'Some Forum' *OR* the subject is not null:

.. code-block:: python

    thread_item.update_item(
        conditional_operator='or',
        forum_name__eq='Some Forum',
        subject__null=False)
    )


Conditional Model.delete
^^^^^^^^^^^^^^^^^^^^^^^^

The following conditional operators are supported for `Model.delete`:

* eq
* ne
* le
* lt
* ge
* gt
* null
* contains
* not_contains
* begins_with
* in
* between

This example will delete the item, only if its `views` attribute is equal to 0.

.. code-block:: python

    print(thread_item.delete(views__eq=0))
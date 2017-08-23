Usage
=====

PynamoDB was written from scratch to be Pythonic, and supports the entire DynamoDB API.

Creating a model
^^^^^^^^^^^^^^^^

Let's create a simple model to describe users.

::

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute

    class UserModel(Model):
        """
        A DynamoDB User
        """
        class Meta:
            table_name = 'dynamodb-user'
            region = 'us-west-1'
        email = UnicodeAttribute(hash_key=True)
        first_name = UnicodeAttribute()
        last_name = UnicodeAttribute()

Models are backed by DynamoDB tables. In this example, the model has a hash key attribute
that stores the user's email address. Any attribute can be set as a hash key by including the argument
`hash_key=True`. The `region` attribute is not required, and will default to `us-east-1` if not provided.

PynamoDB allows you to create the table:

    >>> UserModel.create_table(read_capacity_units=1, write_capacity_units=1)

Now you can create a user:

    >>> user = UserModel('test@example.com', first_name='Samuel', last_name='Adams')
    dynamodb-user<test@example.com>

To write the user to DynamoDB, just call save:

    >>> user.save()

You can see that the table count has changed:

    >>> UserModel.count()
    1

Attributes can be accessed and set normally:

    >>> user.email
    'test@example.com'
    >>> user.email = 'foo-bar'
    >>> user.email
    'foo-bar

Did another process update the user? We can refresh the user with data from DynamoDB::

    >>> user.refresh()

Ready to delete the user?

    >>> user.delete()

Querying
^^^^^^^^

`PynamoDB` provides an intuitive abstraction over the DynamoDB Query API.
All of the Query API comparison operators are supported.

Suppose you had a table with both a hash key that is the user's last name
and a range key that is the user's first name:

::

    class UserModel(Model):
            """
            A DynamoDB User
            """
            class Meta:
                table_name = 'dynamodb-user'
            email = UnicodeAttribute()
            first_name = UnicodeAttribute(range_key=True)
            last_name = UnicodeAttribute(hash_key=True)

Now, suppose that you want to search the table for users with a last name
'Smith', and first name that begins with the letter 'J':

::

    for user in UserModel.query('Smith', UserModel.first_name.startswith('J')):
        print(user.first_name)

You can combine query terms:

::

    for user in UserModel.query('Smith', UserModel.first_name.startswith('J') | UserModel.email.contains('domain.com')):
        print(user)


Counting Items
^^^^^^^^^^^^^^

You can retrieve the count for queries by using the `count` method:

::

    print(UserModel.count('Smith', UserModel.first_name.startswith('J'))


Counts also work for indexes:

::

    print(UserModel.custom_index.count('my_hash_key'))


Alternatively, you can retrieve the table item count by calling the `count` method without filters:

::

    print(UserModel.count())


Note that the first positional argument to `count()` is a `hash_key`. Although
this argument can be `None`, filters must not be used when `hash_key` is `None`:

::

    # raises a ValueError
    print(UserModel.count(UserModel.first_name == 'John'))

    # returns count of only the matching users
    print(UserModel.count('my_hash_key', UserModel.first_name == 'John'))


Batch Operations
^^^^^^^^^^^^^^^^

`PynamoDB` provides context managers for batch operations.

.. note::

    DynamoDB limits batch write operations to 25 `PutRequests` and `DeleteRequests` combined. `PynamoDB` automatically groups your writes 25 at a time for you.

Let's create a whole bunch of users:

::

    with UserModel.batch_write() as batch:
        for i in range(100):
            batch.save(UserModel('user-{0}@example.com'.format(i), first_name='Samuel', last_name='Adams'))

Now, suppose you want to retrieve all those users:

::

    user_keys = [('user-{0}@example.com'.format(i)) for i in range(100)]
    for item in UserModel.batch_get(user_keys):
        print(item)

Perhaps you want to delete all these users:

::

    with UserModel.batch_write() as batch:
        items = [UserModel('user-{0}@example.com'.format(x)) for x in range(100)]
        for item in items:
            batch.delete(item)

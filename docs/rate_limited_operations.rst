Rate-Limited Operation
================

`Scan`, `Query` and `Count` operations can be rate-limited based on the consumed capacities returned from DynamoDB.
Simply specify the `rate_limit` argument when calling these methods. Rate limited writes are not currently supported, although workarounds are discussed below. 

.. note::

    Rate-limiting is only meant to slow operations down to conform to capacity limitations.
    Rate-limiting can not be used to speed operations up. Specifying a higher rate-limit that exceeds the possible
    writing speed allowed by the environment, will not have any effect.

Example Usage
^^^^^^^^^^^^^

Suppose that you have defined a `User` Model for the examples below.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import (
        UnicodeAttribute
    )


    class User(Model):
        class Meta:
            table_name = 'Users'

        id = UnicodeAttribute(hash_key=True)
        name = UnicodeAttribute(range_key=True)


Here is an example using `rate-limit` in while scaning the `User` model

.. code-block:: python

    # Using only 5 RCU per second
    for user in User.scan(rate_limit = 5):
        print("User id: {}, name: {}".format(user.id, user.name))


Query
^^^^^

You can use `rate-limit` when querying items from your table:

.. code-block:: python

    # Using only 15 RCU per second
    for user in User.query('id1', User.name.startswith('re'), rate_limit = 15):
        print("Query returned user {0}".format(user))


Count
^^^^^

You can use `rate-limit` when counting items in your table:

.. code-block:: python

    # Using only 15 RCU per second
    count = User.count(rate_limit = 15):
    print("Count : {}".format(count))
    
Unsupported Scenarios: Rate limiting Writes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Rate limiting writes are currently not supported. One workaround for this is to keep a count that gets increased based on the time passed, and only batch that number of writes (called Leaky bucket algorithm). The pseudocode below assumes writes
are happening constantly. In the case that they're not, sleeping during low utilization could also be useful.

.. code-block:: python

    def write(items):
    while True:
       allowance= min(
                   allowance + time_passed/RATE_OF_WRITING, 
                   MAX_ALLOWED_RATE
                   )
       number_of_items_to_be_written = min(int(allowance), len(items))
       batch_write(items[:number_of_items_to_be_written])
       allowance -= number_of_items_to_be_written
       items = items[number_of_items_to_be_written:]


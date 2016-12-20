Logging
=======

Logging in PynamoDB uses the standard Python logging facilities. PynamoDB is built on top of ``botocore`` which also
uses standard Python logging facilities. Logging is quite verbose, so you may only wish to enable it for debugging purposes.

Here is an example showing how to enable logging for PynamoDB:

.. code-block:: python

    from __future__ import print_function
    import logging
    from pynamodb.models import Model
    from pynamodb.attributes import (
        UnicodeAttribute, NumberAttribute
    )

    logging.basicConfig()
    log = logging.getLogger("pynamodb")
    log.setLevel(logging.DEBUG)
    log.propagate = True

    class Thread(Model):
        class Meta:
            table_name = 'Thread'

        forum_name = UnicodeAttribute(hash_key=True)
        subject = UnicodeAttribute(range_key=True)
        views = NumberAttribute(default=0)

    # Scan
    for item in Thread.scan():
        print(item)

    # Scan
    for item in Thread.rate_limited_scan():
        print(item)

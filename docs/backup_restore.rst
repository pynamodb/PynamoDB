Table Backups
=============

PynamoDB provides methods for backing up and restoring the items in your table. Items are serialized to and from JSON
encoded strings and files. Only serialized item data are stored in a backup, not any table metadata.

Backing up a table
------------------

To back up a table, you can simply use the provided `dump` method and write the contents to a file.

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

    Thread.dump("thread_backup.json")

Alternatively, you can write the contents to a string.

.. code-block:: python

    content = Thread.dumps()


Restoring from a backup
-----------------------

To restore items from a backup file, simply use the provided `load` method.

.. warning::

    Items contained in a backup *will* overwrite any existing items in your table!

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

    Thread.load("thread_backup.json")

Alternatively, you can also load the contents from a string.

.. code-block:: python

    Thread.loads(content)

.. _local:

Use PynamoDB Locally
====================

Several DynamoDB compatible servers have been written for testing and debugging purposes. PynamoDB can be
used with any server that provides the same API as DynamoDB.

PynamoDB has been tested with two DynamoDB compatible servers, `DynamoDB Local <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html>`_
and `dynalite <https://github.com/mhart/dynalite>`_.

To use a local server, you need to set the ``host`` attribute on your ``Model``'s ``Meta`` class to the hostname and port
that your server is listening on.

.. note::

    If you are using DynamoDB Local and also use ``rate_limited_scan`` on your models, you must also
    set ``allow_rate_limited_scan_without_consumed_capacity`` to ``True`` in the
    `settings file <settings.rst#allow_rate_limited_scan_without_consumed_capacity>`_ (dynalite does not require
    this step because it implements returning of consumed capacity in responses, which is used by ``rate_limited_scan``).

.. note::

    Local implementations of DynamoDB such as DynamoDB Local or dynalite may not be fully featured
    (and I don't maintain either of those packages), so you may encounter errors or bugs with a
    local implementation that you would not encounter using DynamoDB.


.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute


    class Thread(Model):
        class Meta:
            table_name = "Thread"
            host = "http://localhost:8000"
        forum_name = UnicodeAttribute(hash_key=True)

Running dynalite
^^^^^^^^^^^^^^^^

Make sure you have the Node Package Manager installed, instructions `here <https://www.npmjs.org/doc/README.html>`_.

Install dynalite::

    $ npm install -g dynalite

Run dynalite::

    $ dynalite --port 8000

That's it, you've got a DynamoDB compatible server running on port 8000.


Running DynamoDB Local
^^^^^^^^^^^^^^^^^^^^^^

DynamoDB local is a tool provided by Amazon that mocks the DynamoDB API, and uses a local file to
store your data. You can use DynamoDB local with PynamoDB for testing, debugging, or offline development.
For more information, you can read `Amazon's Announcement <http://aws.amazon.com/about-aws/whats-new/2013/09/12/amazon-dynamodb-local/>`_ and
`Jeff Barr's blog post <http://aws.typepad.com/aws/2013/09/dynamodb-local-for-desktop-development.html>`_ about it.

* Download the latest version of DynamoDB local `here <http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest>`_.
* Unpack the contents of the archive into a directory of your choice.

DynamoDB local requires the `Java Runtime Environment <http://java.com/en/>`_ version 7. Make sure the JRE is installed before continuing.

From the directory where you unpacked DynamoDB local, you can launch it like this:

::

    $ java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar

Once the server has started, you should see output:

::

    $ java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar
    2014-03-28 12:09:10.892:INFO:oejs.Server:jetty-8.1.12.v20130726
    2014-03-28 12:09:10.943:INFO:oejs.AbstractConnector:Started SelectChannelConnector@0.0.0.0:8000

Now DynamoDB local is running locally, listening on port 8000 by default.




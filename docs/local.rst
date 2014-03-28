Using DynamoDB Local
====================

DynamoDB local is a tool provided by Amazon that mocks the DynamoDB API, and uses a local file to
store your data. You can use DynamoDB local with PynamoDB for testing, debugging, or offline development.
For more information, you can read `Amazon's Announcement <http://aws.amazon.com/about-aws/whats-new/2013/09/12/amazon-dynamodb-local/>`_ and
`Jeff Barr's blog post <http://aws.typepad.com/aws/2013/09/dynamodb-local-for-desktop-development.html>`_ about it.

Download DynamoDB Local
^^^^^^^^^^^^^^^^^^^^^^^

* Download the latest version of DynamoDB local `here <http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest>`_.
* Unpack the contents of the archive into a directory of your choice.


Running DynamoDB Local
^^^^^^^^^^^^^^^^^^^^^^

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


Using PynamoDB with DynamoDB Local
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All you need to do is set the ``host`` attribute on your ``Model``'s ``Meta`` class to the hostname and port
that DynamoDB Local is listening on - which is probably ``http://localhost:8000``.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import UnicodeAttribute


    class Thread(Model):
        class Meta:
            table_name = 'Thread'
            host = 'http://localhost:8000'
        forum_name = UnicodeAttribute(hash_key=True)

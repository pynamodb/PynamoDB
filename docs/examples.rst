PynamoDB Examples
=================

An directory of examples is available with the PynamoDB source on `GitHub <https://github.com/pynamodb/PynamoDB/tree/devel/examples>`__.
The examples are configured to use `http://localhost:8000` as the DynamoDB endpoint. For information on how to run DynamoDB locally,
see : :ref:`local`.

.. note::

    You should read the examples before executing them. They are configured to use ``http://localhost:8000`` by default, so
    that you can run them without actually consuming DynamoDB resources on AWS, and therefore not costing you any money.

Install PynamoDB
^^^^^^^^^^^^^^^^

Although you can install & run PynamoDB from GitHub, it's best to use a released version from PyPI::

    $ pip install pynamodb


Getting the examples
^^^^^^^^^^^^^^^^^^^^

You can clone the PynamoDB repository to get the examples::

    $ git clone https://github.com/pynamodb/PynamoDB.git

Running the examples
^^^^^^^^^^^^^^^^^^^^

Go into the examples directory::

    $ cd pynamodb/examples

Configuring the examples
^^^^^^^^^^^^^^^^^^^^^^^^

Each example is configured to use ``http://localhost:8000`` as the DynamoDB endpoint. You'll need
to edit an example and either remove the ``host`` setting (causing PynamoDB to use a default), or
specify your own.

Running an example
^^^^^^^^^^^^^^^^^^

Each example file can be executed as a script by a Python interpreter::

    $ python model.py

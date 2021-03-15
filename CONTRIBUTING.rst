Contributing
============

Environment Setup
-----------------

You'll need a python3 installation and a virtualenv. There are many ways to manage
virtualenvs, but a minimal example is shown below.

.. code-block:: bash

  $ virtualenv -p python3 venv && source venv/bin/activate
  $ pip install -e .[signals] -r requirements-dev.txt


A java runtime is required to run the integration tests. After installing java, download and untar the
mock dynamodb server like so:

.. code-block:: bash

  $ wget --quiet http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz -O /tmp/dynamodb_local_latest.tar.gz
  $ tar -xzf /tmp/dynamodb_local_latest.tar.gz -C /tmp

Note that you may want to place files somewhere other than ``/tmp``.


Running Tests
-------------

After installing requirements in environment setup and ensuring your venv is activated, unit tests are run with:

.. code-block:: bash

  $ pytest tests/ -k "not ddblocal"


There are also a set of integration tests that require a local dynamodb server to be mocked.

.. code-block:: bash

  $ java -Djava.library.path=/tmp/DynamoDBLocal_lib -jar /tmp/DynamoDBLocal.jar -inMemory -port 8000
  $ pytest tests/      # in another window


Backwards Compatibility
-----------------------

Particular care should be paid to backwards compatibility when making any change in PynamoDB, especially
with attributes and serialization/deserialization. Consider data written with an older version of the
library and whether it can still be read after upgrading.

Where possible, write logic to continue supporting older data for at least one major version to simplify
the upgrade path. Where that's not possible, create a new version of the attribute with a different name
and mark the old one as deprecated.

Outside of data compatibility, follow the usual semver rules for API changes and limit breaking changes
to a major release.


Pull Requests
-------------

Pull requests should:

#. Specify an accurate title and detailed description of the change
#. Add unit tests
#. Add test coverage for new code (CI will verify the delta)
#. Add type annotations to any code modified
#. Write documentation for new features
#. Maintain the existing code style and patterns


Changelog
---------

Any non-trivial change should be documented in the
`release notes <https://pynamodb.readthedocs.io/en/latest/release_notes.html>`_.
Please include sufficient detail in the PR description, which will be used by
maintainers to populate the release notes.


Documentation
-------------

Docs are built using `sphinx <https://www.sphinx-doc.org/en/1.5.1/>`_ and
available on `readthedocs <https://pynamodb.readthedocs.io/>`_. A release
of the `latest` tag (tracking master) happens automatically on merge via
a Github webhook.

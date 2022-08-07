Contributing
============

Pull requests are welcome, forking from the ``master`` branch. If you are new to GitHub, be sure and check out
GitHub's `Hello World <https://guides.github.com/activities/hello-world/>`_ tutorial.


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


The Scope of the Library
------------------------

The purpose of this library is to provide a Pythonic ODM layer on top of DynamoDB to be used
in server applications' runtime, i.e. to enable their various application logic and features.
While striving for the library to be useful, we're also trying to "do one thing well". For this reason:

- Database administration tasks are out of scope, and while PynamoDB has functions for
  operations like CreateTable, CreateIndex and DeleteTable, it's because they are useful
  for interacting with dynamodb-local and moto's DynamoDB backend from within test code.
  
  For this reason, features such as enabling PITR backups, restoring from such backups,
  updating indices, etc. are intentionally absent. For getting started and operating
  on a small scale, AWS Console and the AWS Command Line Interface (awscli) can be used.
  For larger scale, infrastructure provisioning by dedicated tools (such as CloudFormation
  or Terraform) would be vastly preferable over anything PynamoDB could offer.
  
  Per security best practices, we recommend running your application's runtime with an IAM role
  having the least privileges necessary for it to function (which likely excludes any database
  administration operations).

- While the library aims to empower application developers, it steers away from high-level features
  which are not specific to DynamoDB. For example, a custom attribute which serializes UUIDs
  as strings is doubtlessly something many applications have had a need for, but as long as it doesn't
  exercise any core DynamoDB functionality (e.g. in the case of a UUID attribute, there isn't
  a dedicated DynamoDB data type or API feature for storing UUIDs), we would recommend relegating
  such functionality to auxiliary libraries. One such library is `pynamodb-attributes <https://github.com/lyft/pynamodb-attributes>`_.


Pull Requests
-------------

Pull requests should:

#. Specify an accurate title and detailed description of the change
#. Include thorough testing. Unit tests at a minimum, sometimes integration tests
#. Add test coverage for new code (CI will verify the delta)
#. Add type annotations to any code modified
#. Write documentation for new features
#. Maintain the existing code style (mostly PEP8) and patterns


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

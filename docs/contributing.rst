Contributing
============

Pull requests are welcome, forking from the ``master`` branch. If you are new to GitHub, be sure and check out
GitHub's `Hello World <https://guides.github.com/activities/hello-world/>`_ tutorial.

Make sure that your contribution meets the following requirements:
* Be thoroughly tested
* Works on all supported versions of Python
* Be in the same code style of the existing source code (mostly PEP8)


Testing
^^^^^^^

The PynamoDB source code is thoroughly tested, which helps ensure that with each change made to it, we aren't breaking
someone's code that relies on PynamoDB. It's not easy, and it's not optional. Changes without proper testing won't be
accepted.

Please write tests to accompany your changes, and verify that the tests pass using all supported version of Python
by using ``tox``::

    $ tox

Once you've opened a pull request on GitHub, Travis-ci will run the test suite as well.

Don't forget to add yourself to `AUTHORS.rst <https://github.com/pynamodb/PynamoDB/blob/devel/AUTHORS.rst>`_.

==========================
A very short URL shortener
==========================

This is a very small implementation of a `URL shortener <http://en.wikipedia.org/wiki/URL_shortening>`_ powered by Flask and PynamoDB.

Try it for yourself in three easy steps, assuming you have `access to AWS <https://pynamodb.readthedocs.io/en/latest/awsaccess.html>`_.

Install Requirements
====================
::

    $ pip install flask pynamodb

Run the server
==============
::

    $ python shortener.py

Shorten URLs
============

Now you can navigate to `http://localhost:5000 <http://localhost:5000>`_ to start shortening URLs.

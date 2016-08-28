"""
Support for the old and outdated Python 2.6
"""
import logging
import unittest


class FailedToRaise(Exception):
    pass


class AssertRaises(object):
    def __init__(self, exc):
        self.expected = exc

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type is None:
            raise Exception(
                "{0} not raised".format(self.expected))

        if not issubclass(exc_type, self.expected):
            return False

        self.exception = exc_value

        return True


class CompatTestCase(unittest.TestCase):

    def assertIsNotNone(self, value, *args):
        self.assertNotEqual(value, None, *args)

    def assertIsNone(self, value, *args):
        self.assertEqual(value, None, *args)

    def assertRaises(self, excClass, callableObj=None, *args, **kwargs):
        context = AssertRaises(excClass)
        if callableObj is None:
            return context
        with context:
            callableObj(*args, **kwargs)

    def assertIsInstance(self, obj, cls, msg=None):
        self.assertTrue(isinstance(obj, cls), msg)

    def assertDictEqual(self, d1, d2, msg=None):
        self.assertIsInstance(d1, dict, 'First argument is not a dictionary')
        self.assertIsInstance(d2, dict, 'Second argument is not a dictionary')
        self.assertTrue(d1 == d2, msg)

    def assertListEqual(self, list1, list2, msg=None):
        self.assertTrue(len(list1) == len(list2) and sorted(list1) == sorted(list2), msg)


class NullHandler(logging.Handler):
    def emit(self, record):
        pass

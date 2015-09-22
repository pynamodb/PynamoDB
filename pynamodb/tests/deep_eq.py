#Copyright (c) 2010-2013 Samuel Sutch [samuel.sutch@gmail.com]
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

import datetime, time, functools, operator, types

default_fudge = datetime.timedelta(seconds=0, microseconds=0, days=0)

def deep_eq(_v1, _v2, datetime_fudge=default_fudge, _assert=False):
  """
  Tests for deep equality between two python data structures recursing 
  into sub-structures if necessary. Works with all python types including
  iterators and generators. This function was dreampt up to test API responses
  but could be used for anything. Be careful. With deeply nested structures
  you may blow the stack.
  
  Options:
            datetime_fudge => this is a datetime.timedelta object which, when
                              comparing dates, will accept values that differ
                              by the number of seconds specified
            _assert        => passing yes for this will raise an assertion error
                              when values do not match, instead of returning 
                              false (very useful in combination with pdb)
  
  Doctests included:
  
  >>> x1, y1 = ({'a': 'b'}, {'a': 'b'})
  >>> deep_eq(x1, y1)
  True
  >>> x2, y2 = ({'a': 'b'}, {'b': 'a'})
  >>> deep_eq(x2, y2)
  False
  >>> x3, y3 = ({'a': {'b': 'c'}}, {'a': {'b': 'c'}})
  >>> deep_eq(x3, y3)
  True
  >>> x4, y4 = ({'c': 't', 'a': {'b': 'c'}}, {'a': {'b': 'n'}, 'c': 't'})
  >>> deep_eq(x4, y4)
  False
  >>> x5, y5 = ({'a': [1,2,3]}, {'a': [1,2,3]})
  >>> deep_eq(x5, y5)
  True
  >>> x6, y6 = ({'a': [1,'b',8]}, {'a': [2,'b',8]})
  >>> deep_eq(x6, y6)
  False
  >>> x7, y7 = ('a', 'a')
  >>> deep_eq(x7, y7)
  True
  >>> x8, y8 = (['p','n',['asdf']], ['p','n',['asdf']])
  >>> deep_eq(x8, y8)
  True
  >>> x9, y9 = (['p','n',['asdf',['omg']]], ['p', 'n', ['asdf',['nowai']]])
  >>> deep_eq(x9, y9)
  False
  >>> x10, y10 = (1, 2)
  >>> deep_eq(x10, y10)
  False
  >>> deep_eq((str(p) for p in xrange(10)), (str(p) for p in xrange(10)))
  True
  >>> str(deep_eq(range(4), range(4)))
  'True'  
  >>> deep_eq(xrange(100), xrange(100))
  True
  >>> deep_eq(xrange(2), xrange(5))
  False
  >>> import datetime
  >>> from datetime import datetime as dt
  >>> d1, d2 = (dt.now(), dt.now() + datetime.timedelta(seconds=4))
  >>> deep_eq(d1, d2)
  False
  >>> deep_eq(d1, d2, datetime_fudge=datetime.timedelta(seconds=5))
  True
  """
  _deep_eq = functools.partial(deep_eq, datetime_fudge=datetime_fudge, 
                               _assert=_assert)
  
  def _check_assert(R, a, b, reason=''):
    if _assert and not R:
      assert 0, "an assertion has failed in deep_eq (%s) %s != %s" % (
        reason, str(a), str(b))
    return R
  
  def _deep_dict_eq(d1, d2):
    k1, k2 = (sorted(d1.keys()), sorted(d2.keys()))
    if k1 != k2: # keys should be exactly equal
      return _check_assert(False, k1, k2, "keys")
    
    return _check_assert(operator.eq(sum(_deep_eq(d1[k], d2[k]) 
                                       for k in k1), 
                                     len(k1)), d1, d2, "dictionaries")
  
  def _deep_iter_eq(l1, l2):
    if len(l1) != len(l2):
      return _check_assert(False, l1, l2, "lengths")
    return _check_assert(operator.eq(sum(_deep_eq(v1, v2) 
                                      for v1, v2 in zip(l1, l2)), 
                                     len(l1)), l1, l2, "iterables")
  
  def op(a, b):
    _op = operator.eq
    if type(a) == datetime.datetime and type(b) == datetime.datetime:
      s = datetime_fudge.seconds
      t1, t2 = (time.mktime(a.timetuple()), time.mktime(b.timetuple()))
      l = t1 - t2
      l = -l if l > 0 else l
      return _check_assert((-s if s > 0 else s) <= l, a, b, "dates")
    return _check_assert(_op(a, b), a, b, "values")

  c1, c2 = (_v1, _v2)
  
  # guard against strings because they are iterable and their
  # elements yield iterables infinitely. 
  # I N C E P T I O N
  for t in types.StringTypes:
    if isinstance(_v1, t):
      break
  else:
    if isinstance(_v1, types.DictType):
      op = _deep_dict_eq
    else:
      try:
        c1, c2 = (list(iter(_v1)), list(iter(_v2)))
      except TypeError:
        c1, c2 = _v1, _v2
      else:
        op = _deep_iter_eq
  
  return op(c1, c2)
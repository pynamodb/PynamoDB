SET = 'SET'
REMOVE = 'REMOVE'
DELETE = 'DELETE'
ADD = 'ADD'


"""
options::

expr is an expression with update, condition, etc.

with m.update(expr) as up:
    m.field = 'hi
    m.field2 = 1
# persists here

with m.update() as up:
    m.field = 'hi'
    m.field2 = 1
    up.condition(expr)
# persists here

m.update(Model.field, 5)
# problems arise here
m.update(Model.map.sub_map.field, 'hello')
"""


class UpdateExpression(object):
    def __init__(self, action=None):
        self.action = action


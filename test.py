import traceback

from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute
)

from wrapt import wrap_function_wrapper


def add_otel_header(req, **kwargs):
    req.headers.add_header('traceparent', 'someval')


def emit_otel_span(resp, **kwargs):
    print('in emit')
    print(resp, kwargs)
    pass


def register_client_events(original_func, instance, args, kwargs):
    traceback.print_stack()
    client = original_func(*args, **kwargs)
    event_sys = client.meta.events
    event_sys.register_first('before-sign.*.*', add_otel_header)
    event_sys.register('before-call.*.*', emit_otel_span)
    #event_sys.register('after-call.*.*', emit_otel_span)
    return client


wrap_function_wrapper(
    "botocore.client",
    "ClientCreator.create_client",
    register_client_events,
)


class MyModel(Model):
    foo = UnicodeAttribute(hash_key=True)
    class Meta:
        table_name = "actual_model"
        region = "us-east-1"
        host = "http://localhost:9204"


if not MyModel.exists():
    MyModel.create_table(wait=True, read_capacity_units=1, write_capacity_units=1)

z = MyModel.get("key")

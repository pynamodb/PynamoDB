import timeit
import io
import logging
import zlib
from datetime import datetime


COUNT = 1000

benchmark_results = []
benchmark_registry = {}


def register_benchmark(testname):
    def _wrap(func):
        benchmark_registry[testname] = func
        return func
    return _wrap

def results_new_benchmark(name):
    benchmark_results.append((name, {}))
    print(name)


def results_record_result(callback, count):
    callback_name = callback.__name__
    bench_name = callback_name.split('_', 1)[-1]
    try:
        results = timeit.repeat(
            f"{callback_name}()",
            setup=f"from __main__ import patch_urllib3, {callback_name}; patch_urllib3()",
            repeat=10,
            number=count,
        )
    except Exception:
        logging.exception(f"error running {bench_name}")
        return
    result = count / min(results)
    benchmark_results.append((bench_name, str(result)))

    print(
        "{}: {:,.02f} calls/sec".format(
            bench_name, result
        )
    )


# =============================================================================
# Monkeypatching
# =============================================================================

import urllib3

def mock_urlopen(self, method, url, body, headers, **kwargs):
    #print("IN URLOPEN", method, url, headers, body)

    target = headers.get('X-Amz-Target')
    if target.endswith(b'DescribeTable'):
        body = """{
            "Table": {
                "TableName": "users",
                "TableArn": "arn",
                "CreationDateTime": "1421866952.062",
                "ItemCount": 0,
                "TableSizeBytes": 0,
                "TableStatus": "ACTIVE",
                "ProvisionedThroughput": {
                    "NumberOfDecreasesToday": 0,
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 25
                },
                "AttributeDefinitions": [{"AttributeName": "user_name", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "user_name", "KeyType": "HASH"}],
                "LocalSecondaryIndexes": [],
                "GlobalSecondaryIndexes": []
            }
        }
        """
    elif target.endswith(b'GetItem'):
        # TODO: sometimes raise exc
        body = """{
            "Item": {
                "user_name": {"S": "some_user"},
                "email": {"S": "some_user@gmail.com"},
                "first_name": {"S": "John"},
                "last_name": {"S": "Doe"},
                "phone_number": {"S": "4155551111"},
                "country": {"S": "USA"},
                "preferences": {
                    "M": {
                        "timezone": {"S": "America/New_York"},
                        "allows_notifications": {"BOOL": 1},
                        "date_of_birth": {"S": "2022-10-26T20:00:00.000000+0000"}
                    }
                },
                "last_login": {"S": "2022-10-27T20:00:00.000000+0000"}
            }
        }
        """
    elif target.endswith(b'PutItem'):
        body = """{
            "Attributes": {
                "user_name": {"S": "some_user"},
                "email": {"S": "some_user@gmail.com"},
                "first_name": {"S": "John"},
                "last_name": {"S": "Doe"},
                "phone_number": {"S": "4155551111"},
                "country": {"S": "USA"},
                "preferences": {
                    "M": {
                        "timezone": {"S": "America/New_York"},
                        "allows_notifications": {"BOOL": 1},
                        "date_of_birth": {"S": "2022-10-26T20:44:49.207740+0000"}
                    }
                },
                "last_login": {"S": "2022-10-27T20:00:00.000000+0000"}
            }
        }
        """
    else:
        body = ""

    body_bytes = body.encode('utf-8')
    headers = {
        "content-type": "application/x-amz-json-1.0",
        "content-length": str(len(body_bytes)),
        "x-amz-crc32": str(zlib.crc32(body_bytes)),
        "x-amz-requestid": "YB5DURFL1EQ6ULM39GSEEHFTYTPBBUXDJSYPFZPR4EL7M3AYV0RS",
    }

    # TODO: consumed capacity?

    body = io.BytesIO(body_bytes)
    resp = urllib3.HTTPResponse(
        body,
        preload_content=False,
        headers=headers,
        status=200,
    )
    resp.chunked = False
    return resp


def patch_urllib3():
    urllib3.connectionpool.HTTPConnectionPool.urlopen = mock_urlopen


# =============================================================================
# Setup
# =============================================================================

import os
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, BooleanAttribute, MapAttribute, NumberAttribute, UTCDateTimeAttribute


os.environ["AWS_ACCESS_KEY_ID"] = "1"
os.environ["AWS_SECRET_ACCESS_KEY"] = "1"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


class UserPreferences(MapAttribute):
    timezone = UnicodeAttribute()
    allows_notifications = BooleanAttribute()
    date_of_birth = UTCDateTimeAttribute()


class UserModel(Model):
    class Meta:
        table_name = 'User'
        max_retry_attempts = 0  # TODO: do this conditionally. need to replace the connection object
    user_name = UnicodeAttribute(hash_key=True)
    first_name = UnicodeAttribute()
    last_name = UnicodeAttribute()
    phone_number = UnicodeAttribute()
    country = UnicodeAttribute()
    email = UnicodeAttribute()
    preferences = UserPreferences(null=True)
    last_login = UTCDateTimeAttribute()


# =============================================================================
# GetItem
# =============================================================================

@register_benchmark("get_item")
def bench_get_item():
    UserModel.get("username")


# =============================================================================
# PutItem
# =============================================================================

@register_benchmark("put_item")
def bench_put_item():
    UserModel(
        "username",
        email="some_user@gmail.com",
        first_name="John",
        last_name="Doe",
        phone_number="4155551111",
        country="USA",
        preferences=UserPreferences(
            timezone="America/New_York",
            allows_notifications=True,
            date_of_birth=datetime.utcnow(),
        ),
        last_login=datetime.utcnow(),
    ).save()


# =============================================================================
# Benchmarks.
# =============================================================================

def main():
    results_new_benchmark("Basic operations")

    results_record_result(benchmark_registry["get_item"], COUNT)
    results_record_result(benchmark_registry["put_item"], COUNT)

    print()
    print("Above metrics are in call/sec, larger is better.")




if __name__ == "__main__":
    main()

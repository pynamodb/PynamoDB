# Handling AttributeDict objects in PynamoDB

PynamoDB has been extended to support serialization of `AttributeDict` objects, such as those returned by the [python-dandelion-eu](https://github.com/SpazioDati/python-dandelion-eu) client.

## Usage Example

```python
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, MapAttribute
from uuid import uuid4

class DResponse(Model):
    class Meta:
        table_name = 'your_table_name'
    uuid = UnicodeAttribute(hash_key=True)
    response = MapAttribute()

# Get a response from the Dandelion API
from dandelion import DataTXT
datatxt = DataTXT(token='your_token')
response = datatxt.nex('The doctor says an apple is better than an orange')

# Save the response directly
DResponse(uuid=str(uuid4()), response=response).save()
```

The patch allows PynamoDB to treat `AttributeDict` objects as if they were standard Python dictionaries, enabling direct serialization without needing to convert to JSON and back.

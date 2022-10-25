"""
Type-annotates the private botocore APIs that we're currently relying on.
"""
from typing import Any, Dict

import botocore.client
import botocore.credentials
import botocore.endpoint
import botocore.hooks
import botocore.model
import botocore.signers


class BotocoreEndpointPrivate(botocore.endpoint.Endpoint):
    _event_emitter: botocore.hooks.HierarchicalEmitter


class BotocoreRequestSignerPrivate(botocore.signers.RequestSigner):
    _credentials: botocore.credentials.Credentials


class BotocoreBaseClientPrivate(botocore.client.BaseClient):
    _endpoint: BotocoreEndpointPrivate
    _request_signer: BotocoreRequestSignerPrivate
    _service_model: botocore.model.ServiceModel

    def _convert_to_request_dict(self, api_params: Dict[str, Any], operation_model: botocore.model.OperationModel, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        ...

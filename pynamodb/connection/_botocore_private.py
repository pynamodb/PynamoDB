"""
Type-annotates the private botocore APIs that we're currently relying on.
"""
from typing import Dict

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

    def _make_api_call(
        self,
        operation_name: str,
        operation_kwargs: Dict,
    ) -> Dict:
        raise NotImplementedError

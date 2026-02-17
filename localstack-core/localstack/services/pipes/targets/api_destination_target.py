import base64
import logging
import re

import requests

from localstack.aws.connect import connect_to
from localstack.services.lambda_.event_source_mapping.pipe_utils import to_json_str
from localstack.services.pipes.targets.pipe_target import PipeTarget
from localstack.utils.aws.arns import extract_account_id_from_arn, extract_region_from_arn
from localstack.utils.aws.message_forwarding import add_target_http_parameters
from localstack.utils.strings import to_str

LOG = logging.getLogger(__name__)


class ApiDestinationPipeTarget(PipeTarget):
    def target_service(self) -> str:
        return "events"

    def send(self, events: list[dict]) -> None:
        target_region = extract_region_from_arn(self.target_arn)
        target_account_id = extract_account_id_from_arn(self.target_arn)
        api_destination_name = self.target_arn.split(":")[-1].split("/")[1]

        events_client = connect_to(
            aws_access_key_id=target_account_id, region_name=target_region
        ).events
        destination = events_client.describe_api_destination(Name=api_destination_name)

        method = destination.get("HttpMethod", "GET")
        endpoint = destination.get("InvocationEndpoint")

        LOG.debug("Pipe API destination %s: %s %s", api_destination_name, method, endpoint)

        headers = {
            "User-Agent": "Amazon/EventBridge/ApiDestinations",
            "Content-Type": "application/json; charset=utf-8",
            "Range": "bytes=0-1048575",
            "Accept-Encoding": "gzip,deflate",
            "Connection": "close",
        }

        # Apply connection auth headers directly from the LocalStack events store
        # (bypasses moto-dependent add_api_destination_authorization)
        connection_arn = destination.get("ConnectionArn", "")
        if connection_arn:
            self._apply_connection_auth(connection_arn, headers)

        http_params = self.target_parameters.get("HttpParameters") if self.target_parameters else None
        if http_params:
            endpoint = add_target_http_parameters(http_params, endpoint, headers, {})

        for event in events:
            payload = to_json_str(event)
            LOG.debug("Pipe API destination %s payload: %s", api_destination_name, payload)
            result = requests.request(
                method=method,
                url=endpoint,
                data=payload,
                headers=headers,
            )
            if result.status_code >= 400:
                LOG.warning(
                    "Received code %s forwarding pipe event: %s %s response: %s",
                    result.status_code,
                    method,
                    endpoint,
                    result.text[:500],
                )

    @staticmethod
    def _apply_connection_auth(connection_arn: str, headers: dict) -> None:
        """Fetch connection auth from the events store and SecretsManager, then apply headers.

        The Connection model stores public parameters only (secrets are stripped).
        The actual secret values (ApiKeyValue, Password) are stored in SecretsManager
        under the connection's secret_arn.
        """
        try:
            from localstack.services.events.models import events_stores

            connection_region = extract_region_from_arn(connection_arn)
            connection_account = extract_account_id_from_arn(connection_arn)
            match = re.search(r"connection/([^/]+)", connection_arn)
            if not match:
                LOG.warning("Could not parse connection name from ARN: %s", connection_arn)
                return
            connection_name = match.group(1)

            store = events_stores[connection_account][connection_region]
            connection = store.connections.get(connection_name)
            if not connection:
                LOG.warning("Connection %s not found in events store", connection_name)
                return

            # Fetch the full auth parameters (including secrets) from SecretsManager
            auth_params = _get_connection_secret(
                connection.secret_arn, connection_region, connection_account
            )
            if not auth_params:
                LOG.warning("Could not retrieve secret for connection %s", connection_name)
                return

            auth_type = (connection.authorization_type or "").upper()

            if auth_type == "API_KEY":
                api_key_params = auth_params.get("ApiKeyAuthParameters", {})
                key_name = api_key_params.get("ApiKeyName", "")
                key_value = api_key_params.get("ApiKeyValue", "")
                if key_name and key_value:
                    headers[key_name] = key_value

            elif auth_type == "BASIC":
                basic_params = auth_params.get("BasicAuthParameters", {})
                username = basic_params.get("Username", "")
                password = basic_params.get("Password", "")
                auth = "Basic " + to_str(
                    base64.b64encode(f"{username}:{password}".encode("ascii"))
                )
                headers["authorization"] = auth

            elif auth_type == "OAUTH_CLIENT_CREDENTIALS":
                LOG.debug("OAuth auth type not fully implemented for Pipes API destinations")

        except Exception:
            LOG.warning("Failed to apply connection auth for %s", connection_arn, exc_info=True)


def _get_connection_secret(secret_arn: str, region: str, account_id: str) -> dict | None:
    """Retrieve the full auth parameters from SecretsManager."""
    if not secret_arn:
        return None
    try:
        import json as _json

        secretsmanager_client = connect_to(
            aws_access_key_id=account_id, region_name=region
        ).secretsmanager
        response = secretsmanager_client.get_secret_value(SecretId=secret_arn)
        return _json.loads(response.get("SecretString", "{}"))
    except Exception as e:
        LOG.debug("Failed to retrieve connection secret %s: %s", secret_arn, e)
        return None

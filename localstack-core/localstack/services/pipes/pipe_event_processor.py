import base64
import json
import logging

from localstack.services.lambda_.event_source_mapping.event_processor import (
    CustomerInvocationError,
    EventProcessor,
    PipeInternalError,
)
from localstack.services.pipes.targets.input_transformer import PipeInputTransformer
from localstack.services.pipes.targets.pipe_target import PipeTarget
from localstack.utils.strings import to_str

LOG = logging.getLogger(__name__)


class PipeEventProcessor(EventProcessor):
    """Processes events from a Pipe source and sends them to a Pipe target."""

    def __init__(
        self,
        target: PipeTarget,
        input_transformer: PipeInputTransformer | None = None,
    ):
        self.target = target
        self.input_transformer = input_transformer

    def process_events_batch(self, input_events: list[dict]) -> None:
        """Process a batch of events: optionally transform, then send to target."""
        try:
            events = [_decode_data_field(e) for e in input_events]

            if self.input_transformer:
                transformed = [self.input_transformer.transform(e) for e in events]
            else:
                transformed = events

            self.target.send(transformed)
        except CustomerInvocationError:
            raise
        except Exception as e:
            LOG.warning("Pipe target invocation failed: %s", e, exc_info=True)
            raise PipeInternalError(str(e)) from e

    def generate_event_failure_context(self, abort_condition: str, **kwargs) -> dict:
        """Generate failure context for DLQ."""
        return {
            "condition": abort_condition,
            "targetArn": self.target.target_arn,
            "error": kwargs.get("error", "Unknown"),
        }


def _decode_data_field(event: dict) -> dict:
    """Decode the base64 'data' field in Kinesis/DynamoDB events.

    AWS Pipes decodes record data before applying InputTemplate or forwarding
    to targets, unlike Lambda ESM which passes base64-encoded data through.
    The data may be double base64-encoded because LocalStack's internal Kinesis
    client doesn't decode the Data field the way standard boto3 does, and then
    KinesisPoller base64-encodes it again. We decode iteratively until we reach
    the original payload.
    """
    if "data" not in event:
        return event
    event = dict(event)
    raw = event["data"]
    try:
        # Iteratively decode base64 layers (typically 2 in LocalStack)
        decoded = raw
        for _ in range(3):
            try:
                decoded = to_str(base64.b64decode(decoded))
            except Exception:
                break
            # Try to parse as JSON — if it works, we've reached the payload
            try:
                event["data"] = json.loads(decoded)
                return event
            except (json.JSONDecodeError, ValueError):
                continue
        # Exhausted decode attempts — use the last successfully decoded string
        event["data"] = decoded
    except Exception as e:
        LOG.debug("Failed to decode data field, passing through as-is: %s", e)
    return event


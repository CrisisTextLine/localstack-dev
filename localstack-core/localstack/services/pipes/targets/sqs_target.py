import json
import logging

from localstack.services.lambda_.event_source_mapping.pipe_utils import (
    get_internal_client,
    to_json_str,
)
from localstack.services.pipes.targets.pipe_target import PipeTarget
from localstack.utils.aws.arns import sqs_queue_url_for_arn
from localstack.utils.aws.client_types import ServicePrincipal

LOG = logging.getLogger(__name__)


class SqsPipeTarget(PipeTarget):
    def target_service(self) -> str:
        return "sqs"

    def send(self, events: list[dict]) -> None:
        sqs_client = get_internal_client(
            self.target_arn,
            role_arn=self.role_arn,
            service_principal=ServicePrincipal.pipes,
            source_arn=self.target_arn,
            service="sqs",
        )

        queue_url = sqs_queue_url_for_arn(self.target_arn)
        sqs_params = self.target_parameters.get("SqsQueueParameters", {})

        for event in events:
            kwargs = {}
            if msg_group_id := sqs_params.get("MessageGroupId"):
                kwargs["MessageGroupId"] = msg_group_id
            if msg_dedup_id := sqs_params.get("MessageDeduplicationId"):
                kwargs["MessageDeduplicationId"] = msg_dedup_id

            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=to_json_str(event),
                **kwargs,
            )

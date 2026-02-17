import logging

from localstack.services.lambda_.event_source_mapping.pipe_utils import (
    get_internal_client,
    to_json_str,
)
from localstack.services.pipes.targets.pipe_target import PipeTarget
from localstack.utils.aws.client_types import ServicePrincipal

LOG = logging.getLogger(__name__)


class KinesisPipeTarget(PipeTarget):
    def target_service(self) -> str:
        return "kinesis"

    def send(self, events: list[dict]) -> None:
        kinesis_client = get_internal_client(
            self.target_arn,
            role_arn=self.role_arn,
            service_principal=ServicePrincipal.pipes,
            source_arn=self.target_arn,
            service="kinesis",
        )

        stream_name = self.target_arn.split("/")[-1]
        kinesis_params = self.target_parameters.get("KinesisStreamParameters", {})
        partition_key = kinesis_params.get("PartitionKey", "default")

        for event in events:
            kinesis_client.put_record(
                StreamName=stream_name,
                Data=to_json_str(event).encode("utf-8"),
                PartitionKey=partition_key,
            )

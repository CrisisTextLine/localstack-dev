import logging

from localstack.services.lambda_.event_source_mapping.pipe_utils import get_internal_client
from localstack.services.pipes.models import PipeEntity
from localstack.services.pipes.pipe_event_processor import PipeEventProcessor
from localstack.services.pipes.pipe_worker import PipeWorker
from localstack.services.pipes.targets.input_transformer import PipeInputTransformer
from localstack.services.pipes.targets.target_factory import PipeTargetFactory
from localstack.utils.aws.client_types import ServicePrincipal

LOG = logging.getLogger(__name__)


class PipeWorkerFactory:
    @staticmethod
    def create(pipe: PipeEntity) -> PipeWorker:
        """Create a PipeWorker with the appropriate poller, target, and processor."""
        # Create source client with role assumption
        source_client = get_internal_client(
            pipe.source,
            role_arn=pipe.role_arn,
            service_principal=ServicePrincipal.pipes,
            source_arn=pipe.arn,
        )

        # Create target
        target = PipeTargetFactory.create(pipe.target, pipe.target_parameters, pipe.role_arn)

        # Create optional input transformer
        input_transformer = None
        if pipe.target_parameters and pipe.target_parameters.get("InputTemplate"):
            input_transformer = PipeInputTransformer(
                input_template=pipe.target_parameters["InputTemplate"],
                pipe_arn=pipe.arn,
                pipe_name=pipe.name,
                source_arn=pipe.source,
                target_arn=pipe.target,
            )

        # Create event processor
        processor = PipeEventProcessor(target=target, input_transformer=input_transformer)

        # Create poller based on source ARN service
        source_service = _source_service_from_arn(pipe.source)
        source_parameters = pipe.source_parameters or {}
        poller = _create_poller(source_service, pipe.source, source_parameters, source_client, processor)

        return PipeWorker(pipe=pipe, poller=poller)


def _source_service_from_arn(arn: str) -> str:
    """Extract service name from source ARN."""
    parts = arn.split(":")
    if len(parts) >= 3:
        return parts[2]
    return ""


def _create_poller(service: str, source_arn: str, source_parameters: dict, source_client, processor):
    """Create the appropriate poller for the source service."""
    if service == "sqs":
        from localstack.services.lambda_.event_source_mapping.pollers.sqs_poller import SqsPoller

        # Ensure SqsQueueParameters exists for SqsPoller
        if "SqsQueueParameters" not in source_parameters:
            source_parameters["SqsQueueParameters"] = {}

        return SqsPoller(
            source_arn=source_arn,
            source_parameters=source_parameters,
            source_client=source_client,
            processor=processor,
        )

    if service == "kinesis":
        from localstack.services.lambda_.event_source_mapping.pollers.kinesis_poller import (
            KinesisPoller,
        )

        if "KinesisStreamParameters" not in source_parameters:
            source_parameters["KinesisStreamParameters"] = {}
        source_parameters["KinesisStreamParameters"].setdefault("StartingPosition", "TRIM_HORIZON")
        source_parameters["KinesisStreamParameters"].setdefault("BatchSize", 100)

        return KinesisPoller(
            source_arn=source_arn,
            source_parameters=source_parameters,
            source_client=source_client,
            processor=processor,
            kinesis_namespace=False,
        )

    if service == "dynamodb":
        from localstack.services.lambda_.event_source_mapping.pollers.dynamodb_poller import (
            DynamoDBPoller,
        )

        if "DynamoDBStreamParameters" not in source_parameters:
            source_parameters["DynamoDBStreamParameters"] = {}
        source_parameters["DynamoDBStreamParameters"].setdefault("StartingPosition", "TRIM_HORIZON")
        source_parameters["DynamoDBStreamParameters"].setdefault("BatchSize", 100)

        return DynamoDBPoller(
            source_arn=source_arn,
            source_parameters=source_parameters,
            source_client=source_client,
            processor=processor,
        )

    raise ValueError(f"Unsupported source service: {service}")

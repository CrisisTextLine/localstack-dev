from localstack.aws.api.pipes import ValidationException
from localstack.services.pipes.targets.pipe_target import PipeTarget


class PipeTargetFactory:
    @staticmethod
    def create(target_arn: str, target_parameters: dict | None, role_arn: str | None) -> PipeTarget:
        """Create the appropriate PipeTarget based on the target ARN."""
        service = _service_from_arn(target_arn)

        if service == "sqs":
            from localstack.services.pipes.targets.sqs_target import SqsPipeTarget

            return SqsPipeTarget(target_arn, target_parameters, role_arn)

        if service == "kinesis":
            from localstack.services.pipes.targets.kinesis_target import KinesisPipeTarget

            return KinesisPipeTarget(target_arn, target_parameters, role_arn)

        if service == "events" and "api-destination" in target_arn:
            from localstack.services.pipes.targets.api_destination_target import (
                ApiDestinationPipeTarget,
            )

            return ApiDestinationPipeTarget(target_arn, target_parameters, role_arn)

        raise ValidationException(f"Unsupported target: {target_arn}")


def _service_from_arn(arn: str) -> str:
    """Extract the service name from an ARN."""
    # ARN format: arn:aws:service:region:account:resource
    parts = arn.split(":")
    if len(parts) >= 3:
        return parts[2]
    return ""

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

from localstack.aws.api.pipes import (
    Arn,
    ArnOrUrl,
    KmsKeyIdentifier,
    OptionalArn,
    PipeDescription,
    PipeEnrichmentParameters,
    PipeLogConfigurationParameters,
    PipeName,
    PipeSourceParameters,
    PipeState,
    PipeStateReason,
    PipeTargetParameters,
    RequestedPipeState,
    RoleArn,
    TagMap,
    Timestamp,
    ValidationException,
)
from localstack.services.stores import (
    AccountRegionBundle,
    BaseStore,
    CrossRegionAttribute,
    LocalAttribute,
)
from localstack.utils.tagging import TaggingService

PIPE_NAME_PATTERN = re.compile(r"^[\.\-_A-Za-z0-9]+$")
PIPE_NAME_MAX_LENGTH = 64


def _validate_pipe_name(name: PipeName) -> None:
    if not name or len(name) > PIPE_NAME_MAX_LENGTH or not PIPE_NAME_PATTERN.match(name):
        raise ValidationException(
            f"1 validation error detected: Value '{name}' at 'name' failed to satisfy constraint: "
            f"Member must satisfy regular expression pattern: [\\-_A-Za-z0-9]+ and have length between 1 and {PIPE_NAME_MAX_LENGTH}"
        )


def _pipe_arn(name: PipeName, account_id: str, region: str) -> str:
    return f"arn:aws:pipes:{region}:{account_id}:pipe/{name}"


@dataclass
class PipeEntity:
    name: PipeName
    source: ArnOrUrl
    target: Arn
    role_arn: RoleArn
    region: str
    account_id: str
    description: PipeDescription | None = None
    desired_state: RequestedPipeState = RequestedPipeState.RUNNING
    current_state: PipeState = PipeState.CREATING
    state_reason: PipeStateReason | None = None
    source_parameters: PipeSourceParameters | None = None
    target_parameters: PipeTargetParameters | None = None
    enrichment: OptionalArn | None = None
    enrichment_parameters: PipeEnrichmentParameters | None = None
    tags: TagMap | None = None
    log_configuration: PipeLogConfigurationParameters | None = None
    kms_key_identifier: KmsKeyIdentifier | None = None
    creation_time: Timestamp = field(default_factory=lambda: datetime.now(UTC))
    last_modified_time: Timestamp = field(default_factory=lambda: datetime.now(UTC))

    @property
    def arn(self) -> str:
        return _pipe_arn(self.name, self.account_id, self.region)


PipeDict = dict[PipeName, PipeEntity]


class PipesStore(BaseStore):
    pipes: PipeDict = LocalAttribute(default=dict)
    TAGS: TaggingService = CrossRegionAttribute(default=TaggingService)


pipes_stores = AccountRegionBundle("pipes", PipesStore)

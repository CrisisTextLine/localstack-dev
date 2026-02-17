import logging
import re
from datetime import UTC, datetime

from localstack.aws.api import RequestContext
from localstack.aws.api.pipes import (
    Arn,
    ArnOrUrl,
    ConflictException,
    CreatePipeResponse,
    DeletePipeResponse,
    DescribePipeResponse,
    KmsKeyIdentifier,
    LimitMax100,
    ListPipesResponse,
    ListTagsForResourceResponse,
    NextToken,
    NotFoundException,
    OptionalArn,
    Pipe,
    PipeArn,
    PipeDescription,
    PipeEnrichmentParameters,
    PipeLogConfigurationParameters,
    PipeName,
    PipeSourceParameters,
    PipeState,
    PipeTargetParameters,
    PipesApi,
    RequestedPipeState,
    ResourceArn,
    RoleArn,
    StartPipeResponse,
    StopPipeResponse,
    TagKeyList,
    TagMap,
    TagResourceResponse,
    UntagResourceResponse,
    UpdatePipeResponse,
    UpdatePipeSourceParameters,
    ValidationException,
)
from localstack.services.pipes.models import (
    PipeEntity,
    _pipe_arn,
    _validate_pipe_name,
    pipes_stores,
)
from localstack.services.plugins import ServiceLifecycleHook
from localstack.state import StateVisitor

LOG = logging.getLogger(__name__)


class PipesProvider(PipesApi, ServiceLifecycleHook):
    def __init__(self):
        self._pipe_workers: dict[str, object] = {}

    def accept_state_visitor(self, visitor: StateVisitor):
        visitor.visit(pipes_stores)

    def on_before_stop(self):
        for name, worker in list(self._pipe_workers.items()):
            try:
                worker.stop()
            except Exception:
                LOG.warning("Failed to stop pipe worker %s", name, exc_info=True)
        self._pipe_workers.clear()

    def _get_store(self, context: RequestContext):
        return pipes_stores[context.account_id][context.region]

    def _get_pipe(self, context: RequestContext, name: PipeName) -> PipeEntity:
        store = self._get_store(context)
        pipe = store.pipes.get(name)
        if not pipe:
            raise NotFoundException(f"Pipe {name} does not exist.")
        return pipe

    def create_pipe(
        self,
        context: RequestContext,
        name: PipeName,
        source: ArnOrUrl,
        target: Arn,
        role_arn: RoleArn,
        description: PipeDescription | None = None,
        desired_state: RequestedPipeState | None = None,
        source_parameters: PipeSourceParameters | None = None,
        enrichment: OptionalArn | None = None,
        enrichment_parameters: PipeEnrichmentParameters | None = None,
        target_parameters: PipeTargetParameters | None = None,
        tags: TagMap | None = None,
        log_configuration: PipeLogConfigurationParameters | None = None,
        kms_key_identifier: KmsKeyIdentifier | None = None,
        **kwargs,
    ) -> CreatePipeResponse:
        _validate_pipe_name(name)
        store = self._get_store(context)

        if name in store.pipes:
            raise ConflictException(
                f"Pipe {name} already exists.",
                resourceId=name,
                resourceType="pipe",
            )

        if desired_state is None:
            desired_state = RequestedPipeState.RUNNING

        now = datetime.now(UTC)
        pipe = PipeEntity(
            name=name,
            source=source,
            target=target,
            role_arn=role_arn,
            region=context.region,
            account_id=context.account_id,
            description=description,
            desired_state=desired_state,
            current_state=PipeState.CREATING,
            source_parameters=source_parameters,
            target_parameters=target_parameters,
            enrichment=enrichment,
            enrichment_parameters=enrichment_parameters,
            tags=tags,
            log_configuration=log_configuration,
            kms_key_identifier=kms_key_identifier,
            creation_time=now,
            last_modified_time=now,
        )

        store.pipes[name] = pipe

        if tags:
            store.TAGS.tag_resource(pipe.arn, [{"Key": k, "Value": v} for k, v in tags.items()])

        # Worker creation happens in Phase 4; for now transition to STOPPED
        if desired_state == RequestedPipeState.RUNNING:
            self._start_pipe_worker(pipe)
        else:
            pipe.current_state = PipeState.STOPPED

        return CreatePipeResponse(
            Arn=pipe.arn,
            Name=pipe.name,
            DesiredState=pipe.desired_state,
            CurrentState=pipe.current_state,
            CreationTime=pipe.creation_time,
            LastModifiedTime=pipe.last_modified_time,
        )

    def delete_pipe(
        self, context: RequestContext, name: PipeName, **kwargs
    ) -> DeletePipeResponse:
        store = self._get_store(context)
        pipe = self._get_pipe(context, name)

        self._stop_pipe_worker(pipe)

        pipe.current_state = PipeState.DELETING
        pipe.desired_state = "DELETED"
        pipe.last_modified_time = datetime.now(UTC)

        response = DeletePipeResponse(
            Arn=pipe.arn,
            Name=pipe.name,
            DesiredState=pipe.desired_state,
            CurrentState=pipe.current_state,
            CreationTime=pipe.creation_time,
            LastModifiedTime=pipe.last_modified_time,
        )

        store.TAGS.untag_resource(pipe.arn, list((pipe.tags or {}).keys()))
        del store.pipes[name]

        return response

    def describe_pipe(
        self, context: RequestContext, name: PipeName, **kwargs
    ) -> DescribePipeResponse:
        pipe = self._get_pipe(context, name)
        return DescribePipeResponse(
            Arn=pipe.arn,
            Name=pipe.name,
            Description=pipe.description,
            DesiredState=pipe.desired_state,
            CurrentState=pipe.current_state,
            StateReason=pipe.state_reason,
            Source=pipe.source,
            SourceParameters=pipe.source_parameters,
            Enrichment=pipe.enrichment,
            EnrichmentParameters=pipe.enrichment_parameters,
            Target=pipe.target,
            TargetParameters=pipe.target_parameters,
            RoleArn=pipe.role_arn,
            Tags=pipe.tags,
            CreationTime=pipe.creation_time,
            LastModifiedTime=pipe.last_modified_time,
            LogConfiguration=pipe.log_configuration,
            KmsKeyIdentifier=pipe.kms_key_identifier,
        )

    def list_pipes(
        self,
        context: RequestContext,
        name_prefix: PipeName | None = None,
        desired_state: RequestedPipeState | None = None,
        current_state: PipeState | None = None,
        source_prefix: ResourceArn | None = None,
        target_prefix: ResourceArn | None = None,
        next_token: NextToken | None = None,
        limit: LimitMax100 | None = None,
        **kwargs,
    ) -> ListPipesResponse:
        store = self._get_store(context)

        pipes = list(store.pipes.values())

        if name_prefix:
            pipes = [p for p in pipes if p.name.startswith(name_prefix)]
        if desired_state:
            pipes = [p for p in pipes if p.desired_state == desired_state]
        if current_state:
            pipes = [p for p in pipes if p.current_state == current_state]
        if source_prefix:
            pipes = [p for p in pipes if p.source and p.source.startswith(source_prefix)]
        if target_prefix:
            pipes = [p for p in pipes if p.target and p.target.startswith(target_prefix)]

        # Simple pagination
        max_results = limit or 100
        pipe_list = [
            Pipe(
                Name=p.name,
                Arn=p.arn,
                DesiredState=p.desired_state,
                CurrentState=p.current_state,
                StateReason=p.state_reason,
                CreationTime=p.creation_time,
                LastModifiedTime=p.last_modified_time,
                Source=p.source,
                Target=p.target,
                Enrichment=p.enrichment,
            )
            for p in pipes[:max_results]
        ]

        return ListPipesResponse(Pipes=pipe_list)

    def start_pipe(
        self, context: RequestContext, name: PipeName, **kwargs
    ) -> StartPipeResponse:
        pipe = self._get_pipe(context, name)

        if pipe.desired_state == RequestedPipeState.RUNNING:
            raise ConflictException(
                f"Pipe {name} is already in the desired state RUNNING.",
                resourceId=name,
                resourceType="pipe",
            )

        pipe.desired_state = RequestedPipeState.RUNNING
        pipe.last_modified_time = datetime.now(UTC)

        self._start_pipe_worker(pipe)

        return StartPipeResponse(
            Arn=pipe.arn,
            Name=pipe.name,
            DesiredState=pipe.desired_state,
            CurrentState=pipe.current_state,
            CreationTime=pipe.creation_time,
            LastModifiedTime=pipe.last_modified_time,
        )

    def stop_pipe(
        self, context: RequestContext, name: PipeName, **kwargs
    ) -> StopPipeResponse:
        pipe = self._get_pipe(context, name)

        if pipe.desired_state == RequestedPipeState.STOPPED:
            raise ConflictException(
                f"Pipe {name} is already in the desired state STOPPED.",
                resourceId=name,
                resourceType="pipe",
            )

        pipe.desired_state = RequestedPipeState.STOPPED
        pipe.last_modified_time = datetime.now(UTC)

        self._stop_pipe_worker(pipe)

        return StopPipeResponse(
            Arn=pipe.arn,
            Name=pipe.name,
            DesiredState=pipe.desired_state,
            CurrentState=pipe.current_state,
            CreationTime=pipe.creation_time,
            LastModifiedTime=pipe.last_modified_time,
        )

    def tag_resource(
        self, context: RequestContext, resource_arn: PipeArn, tags: TagMap, **kwargs
    ) -> TagResourceResponse:
        store = self._get_store(context)
        # Validate the ARN refers to an existing pipe
        pipe_name = self._pipe_name_from_arn(resource_arn)
        pipe = self._get_pipe(context, pipe_name)

        tag_list = [{"Key": k, "Value": v} for k, v in tags.items()]
        store.TAGS.tag_resource(resource_arn, tag_list)

        # Update the pipe entity tags
        if pipe.tags is None:
            pipe.tags = {}
        pipe.tags.update(tags)

        return TagResourceResponse()

    def untag_resource(
        self, context: RequestContext, resource_arn: PipeArn, tag_keys: TagKeyList, **kwargs
    ) -> UntagResourceResponse:
        store = self._get_store(context)
        pipe_name = self._pipe_name_from_arn(resource_arn)
        pipe = self._get_pipe(context, pipe_name)

        store.TAGS.untag_resource(resource_arn, tag_keys)

        if pipe.tags:
            for key in tag_keys:
                pipe.tags.pop(key, None)

        return UntagResourceResponse()

    def list_tags_for_resource(
        self, context: RequestContext, resource_arn: PipeArn, **kwargs
    ) -> ListTagsForResourceResponse:
        store = self._get_store(context)
        pipe_name = self._pipe_name_from_arn(resource_arn)
        self._get_pipe(context, pipe_name)

        tag_list = store.TAGS.list_tags_for_resource(resource_arn)
        tag_map = {t["Key"]: t["Value"] for t in tag_list.get("Tags", [])}
        return ListTagsForResourceResponse(tags=tag_map)

    def update_pipe(
        self,
        context: RequestContext,
        name: PipeName,
        role_arn: RoleArn,
        description: PipeDescription | None = None,
        desired_state: RequestedPipeState | None = None,
        source_parameters: UpdatePipeSourceParameters | None = None,
        enrichment: OptionalArn | None = None,
        enrichment_parameters: PipeEnrichmentParameters | None = None,
        target: Arn | None = None,
        target_parameters: PipeTargetParameters | None = None,
        log_configuration: PipeLogConfigurationParameters | None = None,
        kms_key_identifier: KmsKeyIdentifier | None = None,
        **kwargs,
    ) -> UpdatePipeResponse:
        pipe = self._get_pipe(context, name)

        # Stop existing worker before updating
        self._stop_pipe_worker(pipe)

        pipe.role_arn = role_arn
        if description is not None:
            pipe.description = description
        if desired_state is not None:
            pipe.desired_state = desired_state
        if source_parameters is not None:
            pipe.source_parameters = source_parameters
        if enrichment is not None:
            pipe.enrichment = enrichment
        if enrichment_parameters is not None:
            pipe.enrichment_parameters = enrichment_parameters
        if target is not None:
            pipe.target = target
        if target_parameters is not None:
            pipe.target_parameters = target_parameters
        if log_configuration is not None:
            pipe.log_configuration = log_configuration
        if kms_key_identifier is not None:
            pipe.kms_key_identifier = kms_key_identifier

        pipe.current_state = PipeState.UPDATING
        pipe.last_modified_time = datetime.now(UTC)

        # Restart if desired state is RUNNING
        effective_desired = desired_state or pipe.desired_state
        if effective_desired == RequestedPipeState.RUNNING:
            self._start_pipe_worker(pipe)
        else:
            pipe.current_state = PipeState.STOPPED

        return UpdatePipeResponse(
            Arn=pipe.arn,
            Name=pipe.name,
            DesiredState=pipe.desired_state,
            CurrentState=pipe.current_state,
            CreationTime=pipe.creation_time,
            LastModifiedTime=pipe.last_modified_time,
        )

    def _start_pipe_worker(self, pipe: PipeEntity) -> None:
        """Start a pipe worker. Implemented in Phase 4."""
        try:
            from localstack.services.pipes.pipe_worker_factory import PipeWorkerFactory

            worker = PipeWorkerFactory.create(pipe)
            self._pipe_workers[pipe.name] = worker
            worker.start()
        except Exception:
            LOG.warning("Failed to start pipe worker for %s", pipe.name, exc_info=True)
            pipe.current_state = PipeState.CREATE_FAILED
            pipe.state_reason = "Failed to start pipe worker"

    def _stop_pipe_worker(self, pipe: PipeEntity) -> None:
        """Stop a pipe worker if running."""
        worker = self._pipe_workers.pop(pipe.name, None)
        if worker:
            try:
                worker.stop()
                pipe.current_state = PipeState.STOPPED
            except Exception:
                LOG.warning("Failed to stop pipe worker for %s", pipe.name, exc_info=True)
                pipe.current_state = PipeState.STOP_FAILED

    @staticmethod
    def _pipe_name_from_arn(arn: str) -> str:
        """Extract pipe name from ARN like arn:aws:pipes:region:account:pipe/name."""
        match = re.match(r"arn:aws:pipes:[^:]+:[^:]+:pipe/(.+)", arn)
        if match:
            return match.group(1)
        return arn

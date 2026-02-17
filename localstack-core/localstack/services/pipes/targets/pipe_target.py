from abc import ABC, abstractmethod


class PipeTarget(ABC):
    """Abstract base class for Pipe targets."""

    def __init__(self, target_arn: str, target_parameters: dict | None, role_arn: str | None):
        self.target_arn = target_arn
        self.target_parameters = target_parameters or {}
        self.role_arn = role_arn

    @abstractmethod
    def send(self, events: list[dict]) -> None:
        """Send a batch of events to the target."""

    @abstractmethod
    def target_service(self) -> str:
        """Return the AWS service name for this target."""

import logging
import os
import threading

from localstack.aws.api.pipes import PipeState
from localstack.services.lambda_.event_source_mapping.pollers.poller import (
    EmptyPollResultsException,
    Poller,
)
from localstack.services.pipes.models import PipeEntity
from localstack.utils.backoff import ExponentialBackoff
from localstack.utils.threads import FuncThread

LOG = logging.getLogger(__name__)

# Poll interval in seconds, configurable via env var.
PIPES_POLL_INTERVAL_SEC = float(os.environ.get("PIPES_POLL_INTERVAL_SEC", "1"))
MAX_BACKOFF_ON_ERROR_SEC = 300


class PipeWorker:
    """Background worker that polls a source and sends events to a target via PipeEventProcessor."""

    def __init__(self, pipe: PipeEntity, poller: Poller):
        self.pipe = pipe
        self.poller = poller
        self._shutdown_event = threading.Event()
        self._poller_thread: FuncThread | None = None

    def start(self) -> None:
        self.pipe.current_state = PipeState.STARTING
        self._shutdown_event.clear()
        self._poller_thread = FuncThread(
            self._poll_loop,
            name=f"pipe-worker-{self.pipe.name}",
        )
        self._poller_thread.start()

    def stop(self) -> None:
        self.pipe.current_state = PipeState.STOPPING
        self._shutdown_event.set()

    def is_running(self) -> bool:
        return (
            self._poller_thread is not None
            and self._poller_thread.is_alive()
            and not self._shutdown_event.is_set()
        )

    def _poll_loop(self, *args, **kwargs) -> None:
        self.pipe.current_state = PipeState.RUNNING

        error_backoff = ExponentialBackoff(
            initial_interval=2, max_interval=MAX_BACKOFF_ON_ERROR_SEC
        )

        poll_interval = PIPES_POLL_INTERVAL_SEC

        while not self._shutdown_event.is_set():
            try:
                self.poller.poll_events()
                error_backoff.reset()
                poll_interval = PIPES_POLL_INTERVAL_SEC
            except EmptyPollResultsException:
                poll_interval = PIPES_POLL_INTERVAL_SEC
            except Exception as e:
                LOG.error(
                    "Error polling pipe %s source %s: %s",
                    self.pipe.name,
                    self.pipe.source,
                    e,
                    exc_info=LOG.isEnabledFor(logging.DEBUG),
                )
                poll_interval = error_backoff.next_backoff()
            finally:
                self._shutdown_event.wait(poll_interval)

        self.poller.close()
        self.pipe.current_state = PipeState.STOPPED

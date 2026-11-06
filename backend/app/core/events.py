"""Internal event bus with bounded concurrency, isolated handlers, retry tracking, and graceful shutdown."""

import asyncio
from datetime import datetime, timezone
from enum import Enum
import inspect
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set
from uuid import uuid4

from app.core.errors import EventHandlingError
from app.core.logging import get_logger

logger = get_logger("events")


class ExecutionStatus(str, Enum):
    """Lifecycle status of an individual event handler execution."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRY_ELIGIBLE = "RETRY_ELIGIBLE"


class Event:
    """Event representation passed between modules.

    Attributes:
        event_id: Unique event identifier.
        event_type: Identifier of the event type (e.g. 'storage.disconnected').
        payload: Event payload dictionary.
        source_module: Module that emitted the event.
        timestamp: Time the event was created.
    """

    def __init__(
        self,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
        source_module: str = "app",
        event_id: Optional[str] = None,
    ) -> None:
        self.event_id = event_id or uuid4().hex
        self.event_type = event_type
        self.payload = payload or {}
        self.source_module = source_module
        self.timestamp = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary view of the event."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "source_module": self.source_module,
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        return f"<Event {self.event_type} id={self.event_id} from={self.source_module}>"


class HandlerRegistration:
    """Internal registration metadata for an event subscriber."""

    def __init__(
        self,
        handler_id: str,
        module: str,
        callback: Callable[[Event], Awaitable[None]],
        retry_condition: Optional[str] = None,
        precondition_check: Optional[Callable[[], Any]] = None,
    ) -> None:
        self.handler_id = handler_id
        self.module = module
        self.callback = callback
        self.retry_condition = retry_condition
        self.precondition_check = precondition_check


class HandlerExecutionRecord:
    """Tracks execution outcome and retry context for an individual handler."""

    def __init__(
        self,
        execution_id: str,
        event: Event,
        handler_id: str,
        module: str,
        retry_condition: Optional[str] = None,
    ) -> None:
        self.execution_id = execution_id
        self.event = event
        self.handler_id = handler_id
        self.module = module
        self.status = ExecutionStatus.PENDING
        self.error: Optional[str] = None
        self.retry_condition = retry_condition
        self.attempt_count = 0
        self.created_at = datetime.now(timezone.utc)
        self.last_attempt_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary view of the execution record."""
        return {
            "execution_id": self.execution_id,
            "event_id": self.event.event_id,
            "event_type": self.event.event_type,
            "handler_id": self.handler_id,
            "module": self.module,
            "status": self.status.value,
            "error": self.error,
            "retry_condition": self.retry_condition,
            "attempt_count": self.attempt_count,
            "created_at": self.created_at.isoformat(),
            "last_attempt_at": self.last_attempt_at.isoformat() if self.last_attempt_at else None,
        }

    def __repr__(self) -> str:
        return (
            f"<HandlerExecutionRecord {self.execution_id} "
            f"handler={self.handler_id} status={self.status.value}>"
        )


class EventBus:
    """Internal event bus providing concurrent, isolated handler execution and retry management."""

    def __init__(self, max_concurrency: int = 10) -> None:
        self.max_concurrency = max_concurrency
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._subscribers: Dict[str, List[HandlerRegistration]] = {}
        self._records: Dict[str, HandlerExecutionRecord] = {}
        self._active_dispatches: Set[asyncio.Task] = set()
        self._accepting = True

    def subscribe(
        self,
        event_type: str,
        callback: Callable[[Event], Awaitable[None]],
        handler_id: str,
        module: str,
        retry_condition: Optional[str] = None,
        precondition_check: Optional[Callable[[], Any]] = None,
    ) -> None:
        """Subscribe a handler callback to an event type.

        Args:
            event_type: String identifier of the event (e.g. 'storage.disconnected').
            callback: Async callable accepting the Event instance.
            handler_id: Unique identifier for the handler.
            module: Originating module name.
            retry_condition: Optional state condition tag that makes a failure eligible for retry.
            precondition_check: Optional callable that must return True before retry is attempted.
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []

        registration = HandlerRegistration(
            handler_id=handler_id,
            module=module,
            callback=callback,
            retry_condition=retry_condition,
            precondition_check=precondition_check,
        )
        self._subscribers[event_type].append(registration)
        logger.debug(
            "Subscribed handler [%s:%s] to event type '%s'",
            module,
            handler_id,
            event_type,
        )

    def publish(self, event: Event) -> str:
        """Publish an event asynchronously without blocking the caller (fire-and-forget).

        Args:
            event: Event instance to dispatch.

        Returns:
            The event_id string immediately.

        Raises:
            EventHandlingError: If the EventBus is shutting down.
        """
        if not self._accepting:
            raise EventHandlingError(
                "EventBus is shutting down; cannot accept new events.",
                module="events",
                context={"event_type": event.event_type, "event_id": event.event_id},
            )

        dispatch_task = asyncio.create_task(
            self._dispatch_event(event),
            name=f"dailydock-event-{event.event_type}-{event.event_id[:8]}",
        )
        self._active_dispatches.add(dispatch_task)
        dispatch_task.add_done_callback(self._active_dispatches.discard)

        logger.debug("Published event '%s' (id=%s) non-blocking", event.event_type, event.event_id)
        return event.event_id

    async def publish_and_wait(self, event: Event) -> List[HandlerExecutionRecord]:
        """Publish an event and await completion of all matching handlers.

        Useful for synchronous workflows and test verification.

        Args:
            event: Event instance to dispatch.

        Returns:
            List of HandlerExecutionRecord instances for all registered handlers of this event.

        Raises:
            EventHandlingError: If the EventBus is shutting down.
        """
        if not self._accepting:
            raise EventHandlingError(
                "EventBus is shutting down; cannot accept new events.",
                module="events",
                context={"event_type": event.event_type, "event_id": event.event_id},
            )

        return await self._dispatch_event(event)

    async def _dispatch_event(self, event: Event) -> List[HandlerExecutionRecord]:
        """Dispatch event to all subscribed handlers concurrently with bounded concurrency."""
        handlers = list(self._subscribers.get(event.event_type, []))
        if not handlers:
            logger.debug("No subscribers for event type '%s'", event.event_type)
            return []

        records: List[HandlerExecutionRecord] = []
        tasks = []

        for reg in handlers:
            record_id = uuid4().hex
            record = HandlerExecutionRecord(
                execution_id=record_id,
                event=event,
                handler_id=reg.handler_id,
                module=reg.module,
                retry_condition=reg.retry_condition,
            )
            self._records[record_id] = record
            records.append(record)

            tasks.append(self._execute_single_handler(reg, record))

        # Handlers execute independently; return_exceptions=True isolates failures
        await asyncio.gather(*tasks, return_exceptions=True)
        return records

    async def _execute_single_handler(
        self, reg: HandlerRegistration, record: HandlerExecutionRecord
    ) -> None:
        """Execute a single handler callback bounded by the concurrency semaphore."""
        async with self._semaphore:
            record.status = ExecutionStatus.PENDING
            record.attempt_count += 1
            record.last_attempt_at = datetime.now(timezone.utc)
            try:
                await reg.callback(record.event)
                record.status = ExecutionStatus.SUCCESS
                record.error = None
                logger.debug(
                    "Handler [%s:%s] succeeded for event '%s'",
                    record.module,
                    record.handler_id,
                    record.event.event_type,
                )
            except Exception as exc:
                record.status = ExecutionStatus.FAILED
                record.error = str(exc)
                logger.warning(
                    "Handler [%s:%s] failed for event '%s': %s",
                    record.module,
                    record.handler_id,
                    record.event.event_type,
                    exc,
                )

    def notify_condition(self, condition: str) -> List[str]:
        """Notify the event bus of a condition change.

        Marks matching failed handlers as RETRY_ELIGIBLE and automatically
        triggers their retry execution in the background.

        Args:
            condition: The condition string (e.g. 'storage.connected').

        Returns:
            List of execution_ids that became eligible and were triggered for retry.

        Raises:
            EventHandlingError: If the EventBus is shutting down.
        """
        if not self._accepting:
            raise EventHandlingError(
                "EventBus is shutting down; cannot process condition notifications.",
                module="events",
                context={"condition": condition},
            )

        eligible_ids: List[str] = []
        for record_id, record in self._records.items():
            if (
                record.status in (ExecutionStatus.FAILED, ExecutionStatus.RETRY_ELIGIBLE)
                and record.retry_condition == condition
            ):
                record.status = ExecutionStatus.RETRY_ELIGIBLE
                eligible_ids.append(record_id)
                logger.info(
                    "Execution [%s] for handler [%s:%s] is RETRY_ELIGIBLE for condition '%s'; triggering retry.",
                    record_id,
                    record.module,
                    record.handler_id,
                    condition,
                )

                # Automatically trigger retry execution in the background
                retry_task = asyncio.create_task(
                    self._safe_retry_handler(record_id),
                    name=f"dailydock-retry-{record.handler_id}-{record_id[:8]}",
                )
                self._active_dispatches.add(retry_task)
                retry_task.add_done_callback(self._active_dispatches.discard)

        return eligible_ids

    async def _safe_retry_handler(
        self, execution_id: str
    ) -> Optional[HandlerExecutionRecord]:
        """Safely execute a retry in the background without letting unhandled exceptions leak."""
        try:
            return await self.retry_handler(execution_id)
        except Exception as exc:
            logger.warning(
                "Automatic retry for execution [%s] failed: %s", execution_id, exc
            )
            return None

    async def notify_condition_and_wait(
        self, condition: str
    ) -> List[HandlerExecutionRecord]:
        """Notify of a condition change, automatically trigger retries, and await their completion.

        Args:
            condition: The condition string (e.g. 'storage.connected').

        Returns:
            List of updated HandlerExecutionRecord instances for retried handlers.

        Raises:
            EventHandlingError: If the EventBus is shutting down.
        """
        if not self._accepting:
            raise EventHandlingError(
                "EventBus is shutting down; cannot process condition notifications.",
                module="events",
                context={"condition": condition},
            )

        eligible_ids = [
            record_id
            for record_id, record in self._records.items()
            if record.status in (ExecutionStatus.FAILED, ExecutionStatus.RETRY_ELIGIBLE)
            and record.retry_condition == condition
        ]

        if not eligible_ids:
            return []

        for record_id in eligible_ids:
            self._records[record_id].status = ExecutionStatus.RETRY_ELIGIBLE

        records = await asyncio.gather(
            *[self._safe_retry_handler(record_id) for record_id in eligible_ids],
            return_exceptions=True,
        )
        return [r for r in records if isinstance(r, HandlerExecutionRecord)]

    async def retry_handler(
        self,
        execution_id: str,
        precondition_check: Optional[Callable[[], Any]] = None,
    ) -> HandlerExecutionRecord:
        """Retry a specific failed handler execution using its stored original event.

        Successful handlers are NEVER re-executed as part of this retry.
        Rechecks preconditions before attempting re-execution.

        Args:
            execution_id: Identifier of the failed HandlerExecutionRecord.
            precondition_check: Optional callable override that must return True before retry.

        Returns:
            The updated HandlerExecutionRecord.

        Raises:
            EventHandlingError: If execution_id is unknown or status is not retryable.
        """
        record = self._records.get(execution_id)
        if not record:
            raise EventHandlingError(
                f"Execution record '{execution_id}' not found.",
                module="events",
                context={"execution_id": execution_id},
            )

        if record.status not in (ExecutionStatus.FAILED, ExecutionStatus.RETRY_ELIGIBLE):
            raise EventHandlingError(
                f"Execution record '{execution_id}' has status '{record.status.value}'; not eligible for retry.",
                module="events",
                context={"execution_id": execution_id, "status": record.status.value},
            )

        # Locate handler registration
        handlers = self._subscribers.get(record.event.event_type, [])
        target_reg = next(
            (h for h in handlers if h.handler_id == record.handler_id and h.module == record.module),
            None,
        )

        if not target_reg:
            raise EventHandlingError(
                f"Handler '{record.handler_id}' in module '{record.module}' is no longer registered.",
                module="events",
                context={"handler_id": record.handler_id, "module": record.module},
            )

        # Evaluate precondition check: explicit argument takes precedence over registration check
        check_fn = (
            precondition_check
            if precondition_check is not None
            else target_reg.precondition_check
        )
        if check_fn is not None:
            try:
                is_ready = check_fn()
                if asyncio.iscoroutine(is_ready) or inspect.iscoroutinefunction(check_fn):
                    is_ready = await is_ready
                if not is_ready:
                    logger.warning(
                        "Retry skipped for execution [%s]: precondition check returned False.",
                        execution_id,
                    )
                    return record
            except Exception as exc:
                logger.warning(
                    "Retry skipped for execution [%s]: precondition check raised error: %s",
                    execution_id,
                    exc,
                )
                return record

        # Re-execute ONLY this failed handler with the original event
        logger.info(
            "Retrying failed handler [%s:%s] for event '%s' (attempt %d)...",
            record.module,
            record.handler_id,
            record.event.event_type,
            record.attempt_count + 1,
        )
        await self._execute_single_handler(target_reg, record)
        return record

    def get_execution_record(self, execution_id: str) -> Optional[HandlerExecutionRecord]:
        """Retrieve execution record by execution identifier."""
        return self._records.get(execution_id)

    def list_execution_records(
        self,
        event_id: Optional[str] = None,
        status: Optional[ExecutionStatus] = None,
    ) -> List[HandlerExecutionRecord]:
        """List execution records with optional filtering."""
        records = list(self._records.values())
        if event_id:
            records = [r for r in records if r.event.event_id == event_id]
        if status:
            records = [r for r in records if r.status == status]
        return records

    async def shutdown(self, grace_period: float = 5.0) -> None:
        """Gracefully shut down the event bus.

        1. Stop accepting new events.
        2. Wait for active background dispatches to drain within grace_period.
        3. Cancel any remaining dispatches after timeout.
        4. Clean up internal references.
        """
        self._accepting = False
        active_tasks = [t for t in self._active_dispatches if not t.done()]

        if not active_tasks:
            logger.info("EventBus shutdown: no active event dispatches to drain.")
            return

        logger.info(
            "EventBus shutdown: waiting up to %.1fs for %d active dispatches to finish...",
            grace_period,
            len(active_tasks),
        )

        done, pending = await asyncio.wait(active_tasks, timeout=grace_period)

        if pending:
            logger.warning(
                "EventBus shutdown: %d dispatches did not finish within grace period; cancelling...",
                len(pending),
            )
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        self._active_dispatches.clear()
        logger.info("EventBus shutdown complete.")


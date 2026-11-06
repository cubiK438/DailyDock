"""Unit tests for EventBus dispatch, handler isolation, retries, and graceful shutdown."""

import asyncio
import pytest
from app.core.errors import EventHandlingError
from app.core.events import Event, EventBus, ExecutionStatus


@pytest.mark.anyio
async def test_multiple_handlers_receive_event_and_execute_independently():
    """Multiple handlers should all receive the event; failure in one must not stop the other."""
    bus = EventBus(max_concurrency=5)
    received_a = []
    received_b = []

    async def handler_a(event: Event):
        received_a.append(event.payload["msg"])

    async def handler_b(event: Event):
        received_b.append(event.payload["msg"])
        raise RuntimeError("Simulated failure in handler B")

    bus.subscribe("test.event", handler_a, handler_id="h_a", module="mod_a")
    bus.subscribe("test.event", handler_b, handler_id="h_b", module="mod_b")

    event = Event("test.event", payload={"msg": "hello world"}, source_module="publisher")
    records = await bus.publish_and_wait(event)

    assert len(records) == 2
    rec_a = next(r for r in records if r.handler_id == "h_a")
    rec_b = next(r for r in records if r.handler_id == "h_b")

    assert rec_a.status == ExecutionStatus.SUCCESS
    assert rec_b.status == ExecutionStatus.FAILED
    assert "Simulated failure" in rec_b.error

    assert received_a == ["hello world"]
    assert received_b == ["hello world"]


@pytest.mark.anyio
async def test_non_blocking_publish():
    """publish() should return immediately without waiting for handler completion."""
    bus = EventBus(max_concurrency=5)
    completed = False

    async def slow_handler(event: Event):
        nonlocal completed
        await asyncio.sleep(0.05)
        completed = True

    bus.subscribe("slow.event", slow_handler, handler_id="slow_h", module="test")
    event = Event("slow.event", payload={})

    event_id = bus.publish(event)
    assert event_id == event.event_id
    assert completed is False  # Not finished yet upon return

    # Await completion
    await asyncio.sleep(0.08)
    assert completed is True


@pytest.mark.anyio
async def test_manual_retry_and_selective_execution():
    """Manual retry_handler re-runs ONLY the failed handler with original event after precondition check."""
    bus = EventBus(max_concurrency=5)
    successful_runs = 0
    failing_runs = 0
    storage_connected = False

    async def backup_handler(event: Event):
        nonlocal failing_runs
        failing_runs += 1
        if not storage_connected:
            raise RuntimeError("Storage device unavailable")

    async def notify_handler(event: Event):
        nonlocal successful_runs
        successful_runs += 1

    bus.subscribe(
        "backup.requested",
        backup_handler,
        handler_id="backup_job",
        module="backup",
        retry_condition="storage.connected",
    )
    bus.subscribe(
        "backup.requested",
        notify_handler,
        handler_id="notify_job",
        module="notifications",
    )

    event = Event("backup.requested", payload={"target": "/mnt/hdd1"})
    records = await bus.publish_and_wait(event)

    backup_rec = next(r for r in records if r.handler_id == "backup_job")
    notify_rec = next(r for r in records if r.handler_id == "notify_job")

    assert notify_rec.status == ExecutionStatus.SUCCESS
    assert backup_rec.status == ExecutionStatus.FAILED
    assert successful_runs == 1
    assert failing_runs == 1

    # 1. Precondition check that fails
    precondition_called = False

    def check_precondition():
        nonlocal precondition_called
        precondition_called = True
        return storage_connected

    # Precondition returns False -> retry should be skipped
    await bus.retry_handler(backup_rec.execution_id, precondition_check=check_precondition)
    assert precondition_called is True
    assert failing_runs == 1  # Not run yet!

    # 2. Simulate HDD reconnect -> retry should succeed
    storage_connected = True
    await bus.retry_handler(backup_rec.execution_id, precondition_check=check_precondition)

    assert backup_rec.status == ExecutionStatus.SUCCESS
    assert backup_rec.attempt_count == 2
    assert failing_runs == 2

    # CRUCIAL: Successful handler must NOT have re-run!
    assert successful_runs == 1


@pytest.mark.anyio
async def test_automatic_condition_triggered_retry_and_handler_isolation():
    """Condition change automatically triggers retry for failed handlers only, respecting preconditions."""
    bus = EventBus(max_concurrency=5)
    storage_connected = False
    backup_attempts = 0
    notify_attempts = 0

    def check_storage_precondition() -> bool:
        return storage_connected

    async def backup_handler(event: Event):
        nonlocal backup_attempts
        backup_attempts += 1
        if not storage_connected:
            raise RuntimeError("HDD is disconnected")

    async def notify_handler(event: Event):
        nonlocal notify_attempts
        notify_attempts += 1

    # Subscribe backup with retry_condition and precondition_check registered
    bus.subscribe(
        "backup.requested",
        backup_handler,
        handler_id="backup_service",
        module="backup",
        retry_condition="storage.connected",
        precondition_check=check_storage_precondition,
    )
    # Subscribe notify handler (no retry condition)
    bus.subscribe(
        "backup.requested",
        notify_handler,
        handler_id="notify_service",
        module="notifications",
    )

    event = Event("backup.requested", payload={"path": "/data"})
    records = await bus.publish_and_wait(event)

    backup_rec = next(r for r in records if r.handler_id == "backup_service")
    notify_rec = next(r for r in records if r.handler_id == "notify_service")

    # Initial state
    assert notify_rec.status == ExecutionStatus.SUCCESS
    assert backup_rec.status == ExecutionStatus.FAILED
    assert backup_rec.retry_condition == "storage.connected"
    assert notify_attempts == 1
    assert backup_attempts == 1

    # Case 1: Condition notification fired, but precondition check is still False
    await bus.notify_condition_and_wait("storage.connected")
    assert backup_attempts == 1  # Skipped because precondition returned False!

    # Case 2: Precondition becomes satisfied (HDD connects), condition change occurs
    storage_connected = True
    retried_records = await bus.notify_condition_and_wait("storage.connected")

    assert len(retried_records) == 1
    retried_backup = retried_records[0]
    assert retried_backup.execution_id == backup_rec.execution_id
    assert retried_backup.status == ExecutionStatus.SUCCESS
    assert retried_backup.attempt_count == 2
    assert backup_attempts == 2

    # CRUCIAL: Handler isolation check - notify_handler was NEVER re-executed!
    assert notify_attempts == 1


@pytest.mark.anyio
async def test_automatic_background_condition_retry():
    """Non-blocking notify_condition automatically triggers background retries."""
    bus = EventBus(max_concurrency=5)
    reconnected = False
    ran_retry = False

    def is_reconnected():
        return reconnected

    async def sync_handler(event: Event):
        nonlocal ran_retry
        if not reconnected:
            raise ConnectionError("Network down")
        ran_retry = True

    bus.subscribe(
        "sync.event",
        sync_handler,
        handler_id="sync_h",
        module="sync",
        retry_condition="network.online",
        precondition_check=is_reconnected,
    )

    # Initial publish fails
    records = await bus.publish_and_wait(Event("sync.event"))
    record = records[0]
    assert record.status == ExecutionStatus.FAILED

    # Condition changes to online
    reconnected = True

    # Call fire-and-forget notify_condition
    triggered = bus.notify_condition("network.online")
    assert record.execution_id in triggered

    # Wait briefly for background retry to complete
    await asyncio.sleep(0.05)

    assert ran_retry is True
    assert record.status == ExecutionStatus.SUCCESS
    assert record.attempt_count == 2


@pytest.mark.anyio
async def test_event_bus_shutdown_drains_and_rejects_new():
    """EventBus shutdown should reject new publishes and wait for active dispatches."""
    bus = EventBus(max_concurrency=2)
    handler_completed = False

    async def handler(event: Event):
        nonlocal handler_completed
        await asyncio.sleep(0.03)
        handler_completed = True

    bus.subscribe("drain.event", handler, handler_id="drain_h", module="test")
    event = Event("drain.event")

    bus.publish(event)
    await bus.shutdown(grace_period=0.1)

    assert handler_completed is True

    # Subsequent publish should be rejected
    with pytest.raises(EventHandlingError):
        bus.publish(Event("after.shutdown"))

"""Initial database schema for background tasks and events.

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-11-06 22:30:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0001_initial_schema"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create background_tasks table
    op.create_table(
        "background_tasks",
        sa.Column("task_id", sa.String(length=32), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("module", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("task_id"),
    )
    op.create_index("ix_background_tasks_name", "background_tasks", ["name"])
    op.create_index("ix_background_tasks_module", "background_tasks", ["module"])
    op.create_index("ix_background_tasks_status", "background_tasks", ["status"])

    # 2. Create events table
    op.create_table(
        "events",
        sa.Column("event_id", sa.String(length=32), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("source_module", sa.String(length=64), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("event_id"),
    )
    op.create_index("ix_events_event_type", "events", ["event_type"])

    # 3. Create event_handler_executions table
    op.create_table(
        "event_handler_executions",
        sa.Column("execution_id", sa.String(length=32), nullable=False),
        sa.Column("event_id", sa.String(length=32), nullable=False),
        sa.Column("handler_id", sa.String(length=128), nullable=False),
        sa.Column("module", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("retry_condition", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["events.event_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("execution_id"),
    )
    op.create_index("ix_event_handler_executions_event_id", "event_handler_executions", ["event_id"])
    op.create_index("ix_event_handler_executions_handler_id", "event_handler_executions", ["handler_id"])
    op.create_index("ix_event_handler_executions_status", "event_handler_executions", ["status"])
    op.create_index("ix_event_handler_executions_retry_condition", "event_handler_executions", ["retry_condition"])


def downgrade() -> None:
    op.drop_index("ix_event_handler_executions_retry_condition", table_name="event_handler_executions")
    op.drop_index("ix_event_handler_executions_status", table_name="event_handler_executions")
    op.drop_index("ix_event_handler_executions_handler_id", table_name="event_handler_executions")
    op.drop_index("ix_event_handler_executions_event_id", table_name="event_handler_executions")
    op.drop_table("event_handler_executions")

    op.drop_index("ix_events_event_type", table_name="events")
    op.drop_table("events")

    op.drop_index("ix_background_tasks_status", table_name="background_tasks")
    op.drop_index("ix_background_tasks_module", table_name="background_tasks")
    op.drop_index("ix_background_tasks_name", table_name="background_tasks")
    op.drop_table("background_tasks")


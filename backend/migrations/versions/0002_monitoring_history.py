"""Add infrastructure monitoring sample history.

Revision ID: 0002_monitoring_history
Revises: 0001_initial_schema
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002_monitoring_history"
down_revision: Union[str, None] = "0001_initial_schema"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _create_target_sample_table(
    table_name: str, specific_columns: list[sa.Column]
) -> None:
    op.create_table(
        table_name,
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("target_id", sa.String(length=128), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("check_result", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("consecutive_successes", sa.Integer(), nullable=False),
        sa.Column("last_transition_at", sa.DateTime(timezone=True), nullable=True),
        *specific_columns,
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        f"ix_{table_name}_target_observed_id",
        table_name,
        ["target_id", "observed_at", "id"],
    )


def upgrade() -> None:
    op.create_table(
        "monitor_system_samples",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cpu_percent", sa.Float(), nullable=True),
        sa.Column("memory_total_bytes", sa.Integer(), nullable=True),
        sa.Column("memory_used_bytes", sa.Integer(), nullable=True),
        sa.Column("memory_available_bytes", sa.Integer(), nullable=True),
        sa.Column("memory_percent", sa.Float(), nullable=True),
        sa.Column("uptime_seconds", sa.Float(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("check_result", sa.String(length=16), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("consecutive_successes", sa.Integer(), nullable=False),
        sa.Column("last_transition_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_monitor_system_observed_id", "monitor_system_samples", ["observed_at", "id"]
    )

    _create_target_sample_table(
        "monitor_storage_samples",
        [
            sa.Column("mount_path", sa.String(length=1024), nullable=False),
            sa.Column("mount_source", sa.String(length=1024), nullable=True),
            sa.Column("total_bytes", sa.Integer(), nullable=True),
            sa.Column("used_bytes", sa.Integer(), nullable=True),
            sa.Column("available_bytes", sa.Integer(), nullable=True),
            sa.Column("used_percent", sa.Float(), nullable=True),
        ],
    )
    _create_target_sample_table(
        "monitor_connectivity_samples",
        [
            sa.Column("status_code", sa.Integer(), nullable=True),
            sa.Column("latency_ms", sa.Float(), nullable=True),
        ],
    )
    _create_target_sample_table(
        "monitor_http_samples",
        [
            sa.Column("status_code", sa.Integer(), nullable=True),
            sa.Column("latency_ms", sa.Float(), nullable=True),
        ],
    )
    _create_target_sample_table(
        "monitor_service_samples",
        [
            sa.Column("latency_ms", sa.Float(), nullable=True),
            sa.Column("detail", sa.Text(), nullable=True),
        ],
    )


def downgrade() -> None:
    for table_name in (
        "monitor_service_samples",
        "monitor_http_samples",
        "monitor_connectivity_samples",
        "monitor_storage_samples",
    ):
        op.drop_index(
            f"ix_{table_name}_target_observed_id", table_name=table_name
        )
        op.drop_table(table_name)
    op.drop_index("ix_monitor_system_observed_id", table_name="monitor_system_samples")
    op.drop_table("monitor_system_samples")

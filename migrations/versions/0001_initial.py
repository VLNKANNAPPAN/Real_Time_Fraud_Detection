"""Initial event and decision record schema for later processing checkpoints.

Revision ID: 0001_initial
Revises:
"""

import sqlalchemy as sa
from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("transaction_id", sa.String(128), primary_key=True),
        sa.Column("event_id", sa.String(128), nullable=False, unique=True),
        sa.Column("user_id", sa.String(128), nullable=False, index=True),
        sa.Column("payload_digest", sa.String(64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_table(
        "decisions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "transaction_id",
            sa.String(128),
            sa.ForeignKey("transactions.transaction_id"),
            unique=True,
        ),
        sa.Column("action", sa.String(12), nullable=False),
        sa.Column("reason_codes", sa.JSON(), nullable=False),
        sa.Column("feature_snapshot", sa.JSON(), nullable=True),
        sa.Column("feature_version", sa.String(64), nullable=False),
        sa.Column("model_version", sa.String(64), nullable=True),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("inference_status", sa.String(24), nullable=False),
        sa.Column("feature_included", sa.Boolean(), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("decisions")
    op.drop_table("transactions")

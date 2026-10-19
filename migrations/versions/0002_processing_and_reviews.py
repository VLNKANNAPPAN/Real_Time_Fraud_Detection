"""End-to-end processing metadata and immutable initial review labels.

Revision ID: 0002_processing_and_reviews
Revises: 0001_initial
"""

import sqlalchemy as sa
from alembic import op

revision = "0002_processing_and_reviews"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("transactions", sa.Column("source_topic", sa.String(255)))
    op.add_column("transactions", sa.Column("source_partition", sa.Integer()))
    op.add_column("transactions", sa.Column("source_offset", sa.Integer()))
    op.add_column(
        "transactions", sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="1")
    )
    op.add_column("transactions", sa.Column("last_error", sa.String(255)))
    op.add_column(
        "transactions",
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.add_column("transactions", sa.Column("terminal_at", sa.DateTime(timezone=True)))
    op.add_column("decisions", sa.Column("inference_latency_ms", sa.Float()))
    op.create_table(
        "review_labels",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("decision_id", sa.Integer(), sa.ForeignKey("decisions.id"), nullable=False),
        sa.Column("transaction_id", sa.String(128), nullable=False),
        sa.Column("label", sa.String(16), nullable=False),
        sa.Column("reviewer_id", sa.String(128), nullable=False),
        sa.Column("submission_key", sa.String(128), nullable=False),
        sa.Column("model_version", sa.String(64)),
        sa.Column("feature_version", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("decision_id", "submission_key", name="uq_review_submission"),
        sa.UniqueConstraint("decision_id", name="uq_review_decision_initial_label"),
    )
    op.create_index("ix_review_labels_transaction_id", "review_labels", ["transaction_id"])


def downgrade() -> None:
    op.drop_index("ix_review_labels_transaction_id", table_name="review_labels")
    op.drop_table("review_labels")
    op.drop_column("decisions", "inference_latency_ms")
    for name in (
        "terminal_at",
        "updated_at",
        "last_error",
        "attempt_count",
        "source_offset",
        "source_partition",
        "source_topic",
    ):
        op.drop_column("transactions", name)

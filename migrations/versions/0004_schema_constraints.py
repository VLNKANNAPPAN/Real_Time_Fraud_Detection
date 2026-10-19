"""Enforce terminal, score, and label domains in PostgreSQL.

Revision ID: 0004_schema_constraints
Revises: 0003_delayed_labels
"""

from alembic import op

revision = "0004_schema_constraints"
down_revision = "0003_delayed_labels"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("review_labels", "available_at", nullable=False)
    op.create_foreign_key(
        "fk_review_transaction",
        "review_labels",
        "transactions",
        ["transaction_id"],
        ["transaction_id"],
    )
    op.create_check_constraint(
        "ck_transaction_status", "transactions", "status IN ('PENDING', 'DECIDED')"
    )
    op.create_check_constraint("ck_transaction_attempt_count", "transactions", "attempt_count >= 1")
    op.create_check_constraint(
        "ck_decision_action", "decisions", "action IN ('ALLOW', 'REVIEW', 'BLOCK')"
    )
    op.create_check_constraint(
        "ck_decision_inference_status",
        "decisions",
        "inference_status IN ('SCORED', 'FAILED', 'NOT_RUN')",
    )
    op.create_check_constraint(
        "ck_decision_score", "decisions", "score IS NULL OR (score >= 0 AND score <= 1)"
    )
    op.create_check_constraint(
        "ck_review_label", "review_labels", "label IN ('FRAUD', 'NOT_FRAUD', 'UNSURE')"
    )
    op.create_check_constraint(
        "ck_review_label_source",
        "review_labels",
        "label_source IN ('HUMAN', 'SIMULATED')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_review_label_source", "review_labels", type_="check")
    op.drop_constraint("ck_review_label", "review_labels", type_="check")
    op.drop_constraint("ck_decision_score", "decisions", type_="check")
    op.drop_constraint("ck_decision_inference_status", "decisions", type_="check")
    op.drop_constraint("ck_decision_action", "decisions", type_="check")
    op.drop_constraint("ck_transaction_attempt_count", "transactions", type_="check")
    op.drop_constraint("ck_transaction_status", "transactions", type_="check")
    op.drop_constraint("fk_review_transaction", "review_labels", type_="foreignkey")
    op.alter_column("review_labels", "available_at", nullable=True)

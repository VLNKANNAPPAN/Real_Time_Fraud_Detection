"""Persist per-decision model contribution explanations.

Revision ID: 0005_decision_explanations
Revises: 0004_schema_constraints
"""

import sqlalchemy as sa
from alembic import op

revision = "0005_decision_explanations"
down_revision = "0004_schema_constraints"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "decisions",
        sa.Column("explanations", sa.JSON(), nullable=False, server_default="[]"),
    )


def downgrade() -> None:
    op.drop_column("decisions", "explanations")

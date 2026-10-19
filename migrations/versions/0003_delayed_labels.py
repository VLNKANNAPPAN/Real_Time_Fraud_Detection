"""Add delayed-label source and availability time.

Revision ID: 0003_delayed_labels
Revises: 0002_processing_and_reviews
"""

import sqlalchemy as sa
from alembic import op

revision = "0003_delayed_labels"
down_revision = "0002_processing_and_reviews"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "review_labels",
        sa.Column("label_source", sa.String(24), nullable=False, server_default="HUMAN"),
    )
    op.add_column("review_labels", sa.Column("available_at", sa.DateTime(timezone=True)))
    op.execute("UPDATE review_labels SET available_at = created_at WHERE available_at IS NULL")


def downgrade() -> None:
    op.drop_column("review_labels", "available_at")
    op.drop_column("review_labels", "label_source")

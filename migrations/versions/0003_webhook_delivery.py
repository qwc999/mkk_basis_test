"""Track successful webhook delivery without backfilling old payments."""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "payments", sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade():
    op.drop_column("payments", "webhook_delivered_at")

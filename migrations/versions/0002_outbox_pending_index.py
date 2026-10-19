"""Index pending payment events in polling order."""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_outbox_unpublished_payments",
        "outbox",
        ["created_at", "id"],
        postgresql_where=sa.text("published_at IS NULL AND event_type = 'payments.new'"),
    )


def downgrade():
    op.drop_index("ix_outbox_unpublished_payments", table_name="outbox")

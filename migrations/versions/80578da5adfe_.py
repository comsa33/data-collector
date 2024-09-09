"""empty message

Revision ID: 80578da5adfe
Revises: 50b700fd78c1
Create Date: 2024-09-09 13:30:56.701975

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '80578da5adfe'
down_revision = '50b700fd78c1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('data_sources', 'configurations')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('data_sources', sa.Column('configurations', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=True))
    # ### end Alembic commands ###

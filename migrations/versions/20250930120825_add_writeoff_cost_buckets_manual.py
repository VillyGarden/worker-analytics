from alembic import op
import sqlalchemy as sa

# заполнится из оболочки sed ниже
revision = '25b5e5da6480'
down_revision = '679116768d8f'
branch_labels = None
depends_on = None

def upgrade():
    # Добавляем с server_default='0', чтобы не упасть на существующих строках
    op.add_column('sales_daily', sa.Column('writeoff_cost_total',      sa.Numeric(14, 2), nullable=False, server_default='0'))
    op.add_column('sales_daily', sa.Column('writeoff_cost_defect',     sa.Numeric(14, 2), nullable=False, server_default='0'))
    op.add_column('sales_daily', sa.Column('writeoff_cost_inventory',  sa.Numeric(14, 2), nullable=False, server_default='0'))
    op.add_column('sales_daily', sa.Column('writeoff_cost_other',      sa.Numeric(14, 2), nullable=False, server_default='0'))

    # Сразу уберём server_default, чтобы дальше значения задавались кодом явно
    op.alter_column('sales_daily', 'writeoff_cost_total',     server_default=None)
    op.alter_column('sales_daily', 'writeoff_cost_defect',    server_default=None)
    op.alter_column('sales_daily', 'writeoff_cost_inventory', server_default=None)
    op.alter_column('sales_daily', 'writeoff_cost_other',     server_default=None)

def downgrade():
    op.drop_column('sales_daily', 'writeoff_cost_other')
    op.drop_column('sales_daily', 'writeoff_cost_inventory')
    op.drop_column('sales_daily', 'writeoff_cost_defect')
    op.drop_column('sales_daily', 'writeoff_cost_total')

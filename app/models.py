from datetime import date
from sqlalchemy import Column, Integer, String, Date, Numeric, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship, Mapped, mapped_column
from .db import Base

class Warehouse(Base):
    __tablename__ = "warehouse"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ms_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)  # ID из МойСклад
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    sales_daily: Mapped[list["SalesDaily"]] = relationship("SalesDaily", back_populates="warehouse")

class SalesDaily(Base):
    __tablename__ = "sales_daily"
    __table_args__ = (
        UniqueConstraint("date", "warehouse_id", name="uq_sales_daily_date_wh"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouse.id", ondelete="RESTRICT"), nullable=False)

    # Деньги считаем с копейками, точность 2 знака
    revenue: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)          # сумма продаж (выручка)
    cost: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)             # себестоимость проданных
    discount: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)         # скидки за день
    returns_cost: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)     # возвраты по себестоимости
    receipts_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)            # количество чеков
    inflow_cost: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)      # оприходования по себестоимости

    warehouse: Mapped["Warehouse"] = relationship("Warehouse", back_populates="sales_daily")

    # списания (себестоимость)
    writeoff_cost_total: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    writeoff_cost_defect: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    writeoff_cost_inventory: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)
    writeoff_cost_other: Mapped[float] = mapped_column(Numeric(14, 2), nullable=False, default=0)

import datetime
import enum
from typing import Annotated, Optional

from sqlalchemy import (
    TIMESTAMP,
    CheckConstraint,
    Column,
    Enum,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base, str_256

metadata_obj = MetaData()

workers_table = Table(
    "Employees",
    metadata_obj,
    Column("employee_id", Integer, primary_key=True),
    Column("firstname", String),
    Column("lastname", String),
    Column("birth_date", String),
)
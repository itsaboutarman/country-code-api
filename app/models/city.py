from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String
from app.db.database import Base

class City(Base):
    """
    SQLAlchemy 2.0 model for storing city and country code information.
    Fully compatible with Python type hinting and Pylance.
    """
    __tablename__ = "cities"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    country_code: Mapped[str] = mapped_column(String)
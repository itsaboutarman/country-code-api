from sqlalchemy import Column, Integer, String
from app.db.database import Base

class City(Base):
    """
    SQLAlchemy model for storing city and country code information.
    """
    __tablename__ = "cities"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    country_code = Column(String, nullable=False)
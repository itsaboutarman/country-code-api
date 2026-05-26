from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from app.core.config import settings

# Create the SQLAlchemy engine using the secure database URL from settings
engine = create_engine(settings.database_url)

# Create a session factory for handling database transactions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a Base class for our SQLAlchemy models to inherit from
Base = declarative_base()

def get_db():
    """
    Dependency function that yields a database session.
    Ensures that the session is closed automatically after a request is completed.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
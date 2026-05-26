from fastapi import FastAPI
from app.core.config import settings
from app.db.database import engine, Base
from app.models import city 
from app.api.routes import router as api_router

# Create database tables automatically on startup
Base.metadata.create_all(bind=engine)

# Initialize the FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API for managing city country codes with Redis caching and Kafka logging.",
    version="1.0.0"
)

# Include the API router with a clean prefix
app.include_router(api_router, prefix="/api")

@app.get("/")
def read_root():
    """
    Health check endpoint to verify API functionality.
    """
    return {"message": "Welcome to the Country Code API"}
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.models.city import City
from app.schemas.city import CityCreate, CityResponse

router = APIRouter()

@router.post("/cities/", response_model=CityResponse)
def create_or_update_city(city_in: CityCreate, db: Session = Depends(get_db)):
    """
    Create a new city record or update the country code if the city already exists.
    """
    # Check if the city already exists in the database
    existing_city = db.query(City).filter(City.name == city_in.name).first()
    
    if existing_city:
        # Update existing record
        existing_city.country_code = city_in.country_code
        db.commit()
        db.refresh(existing_city)
        return existing_city
    else:
        # Create new record
        new_city = City(name=city_in.name, country_code=city_in.country_code)
        db.add(new_city)
        db.commit()
        db.refresh(new_city)
        return new_city
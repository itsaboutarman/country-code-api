from pydantic import BaseModel, Field, ConfigDict

class CityBase(BaseModel):
    """
    Base Pydantic schema for City data sharing common attributes.
    """
    name: str = Field(..., description="The unique name of the city")
    country_code: str = Field(..., description="The code representing the country")

class CityCreate(CityBase):
    """
    Schema for creating or updating a city record.
    Matches the input expected from the client.
    """
    pass

class CityResponse(CityBase):
    """
    Schema for API response containing full city details including database ID.
    """
    id: int

    # Enables Pydantic to read SQLAlchemy models directly
    model_config = ConfigDict(from_attributes=True)
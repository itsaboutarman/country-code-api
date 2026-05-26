import time
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from app.db.database import get_db
from app.models.city import City
from app.schemas.city import CityCreate, CityResponse
from app.db.clients import redis_client, kafka_producer
from app.core.config import settings

router = APIRouter(prefix="/cities", tags=["cities"])

# ----------------------------------------
# Helper Functions (Background Tasks)
# ----------------------------------------

def update_lru_cache(city_name: str, country_code: str):
    """
    Updates Redis cache maintaining a strictly 10-item LRU with 10-minute TTL.
    """
    key = f"city:{city_name.lower()}"
    current_timestamp = time.time()
    zset_name = "api_lru_tracker"

    # 1. Save data with 10 minutes (600 seconds) TTL
    redis_client.setex(key, 600, country_code)

    # 2. Add to Sorted Set for LRU tracking (Score is access time)
    redis_client.zadd(zset_name, {key: current_timestamp})

    # 3. Clean up expired items from tracker (older than 10 mins)
    redis_client.zremrangebyscore(zset_name, 0, current_timestamp - 600)

    # 4. Enforce max 10 items rule
    total_items = int(redis_client.zcard(zset_name) or 0)  # type: ignore
    
    if total_items > 10:
        items_to_remove = list(redis_client.zrange(zset_name, 0, total_items - 11))  # type: ignore
        if items_to_remove:
            redis_client.delete(*items_to_remove)  # type: ignore
            redis_client.zrem(zset_name, *items_to_remove)  # type: ignore

def send_kafka_log(is_cache_hit: bool, response_time_ms: float):
    """
    Calculates hit percentage and sends log message to Kafka.
    """
    total_req = redis_client.incr("stats:total_requests")
    
    if is_cache_hit:
        hit_req = redis_client.incr("stats:total_hits")
    else:
        hit_req = int(redis_client.get("stats:total_hits") or 0)

    hit_percentage = (hit_req / total_req) * 100

    log_data = {
        "response_time_ms": round(response_time_ms, 2),
        "cache_status": "Hit" if is_cache_hit else "Miss",
        "cache_hit_percentage": round(hit_percentage, 2)
    }

    if kafka_producer:
        kafka_producer.send(settings.KAFKA_TOPIC, log_data)
        kafka_producer.flush()

# ----------------------------------------
# Endpoints
# ----------------------------------------

@router.post("/", response_model=CityResponse)
def create_or_update_city(city_in: CityCreate, db: Session = Depends(get_db)):
    """
    Stage 2: Insert or update a city record in the database.
    """
    existing_city = db.query(City).filter(City.name == city_in.name).first()
    if existing_city:
        existing_city.country_code = city_in.country_code
        db.commit()
        db.refresh(existing_city)
        return existing_city
    else:
        new_city = City(name=city_in.name, country_code=city_in.country_code)
        db.add(new_city)
        db.commit()
        db.refresh(new_city)
        return new_city

@router.get("/{city_name}")
def get_city_country_code(city_name: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Stage 5: Search city in Redis. If missed, fetch from Postgres. Update cache & log to Kafka.
    """
    start_time = time.time()
    search_key = f"city:{city_name.lower()}"

    # 1. Search in Redis
    cached_code = redis_client.get(search_key)

    if cached_code:
        is_cache_hit = True
        country_code = cached_code
    else:
        is_cache_hit = False
        # 2. Search in Database
        db_city = db.query(City).filter(City.name.ilike(city_name)).first()
        if not db_city:
            raise HTTPException(status_code=404, detail="City not found")
        country_code = db_city.country_code

    # 3. Delegate Cache Update & Kafka Logging to Background
    background_tasks.add_task(update_lru_cache, city_name, country_code)
    response_time_ms = (time.time() - start_time) * 1000
    background_tasks.add_task(send_kafka_log, is_cache_hit, response_time_ms)

    return {
        "city": city_name,
        "country_code": country_code,
        "cache_status": "Hit" if is_cache_hit else "Miss"
    }
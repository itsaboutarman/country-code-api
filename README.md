# country-code-api

A comprehensive data engineering and backend infrastructure project. This system is divided into two main components: a highly performant API with caching and logging mechanisms, and a Big Data streaming pipeline for processing large-scale telecom data.

## Architecture and Technologies

* **Backend Framework:** FastAPI (Python)
* **Relational Database:** PostgreSQL
* **Caching Layer:** Redis (Custom LRU implementation: strictly 10 items max, 10-minute TTL)
* **Message Broker:** Apache Kafka (Asynchronous logging of cache hits/misses)
* **Big Data Processing:** Apache Spark (PySpark Structured Streaming)
* **Object Storage:** MinIO (S3-compatible local storage)
* **Infrastructure:** Docker & Docker Compose
* **Dependency Management:** Poetry

## Project Structure

```text
country-code-api/
|-- app/
|   |-- api/           # API Routers and Endpoints
|   |-- core/          # Configuration and settings
|   |-- db/            # Database, Redis, and Kafka client setups
|   |-- models/        # SQLAlchemy ORM models
|   |-- schemas/       # Pydantic models for validation
|-- data/
|   |-- ref/           # Static reference data (ref.csv)
|   |-- REF_SMS/       # Streaming input data files (CSV)
|-- scripts/
|   |-- spark_job.py   # PySpark Structured Streaming script
|-- CountryCode-City.csv # City and country code mapping reference file
|-- docker-compose.yml # Infrastructure configuration
|-- pyproject.toml     # Poetry dependencies
|-- README.md
```

## Prerequisites

Before running the project, ensure you have the following installed on your system:

1. **Docker & Docker Compose:** For running PostgreSQL, Redis, Kafka, and MinIO.
2. **Python 3.x:** Installed and added to PATH.
3. **Poetry:** For managing Python dependencies.
4. **Java 17:** Strictly required for running Apache Spark locally. Ensure `JAVA_HOME` is set properly.

## Setup and Execution Guide

### 1. Infrastructure Setup

Start all required services using Docker Compose. This will spin up PostgreSQL, Redis, Kafka, and MinIO.

```bash
docker-compose up -d
```

Once the containers are up and running, the services and their respective management dashboards are accessible via these local ports:

| Service | Port | URL / Connection | Description |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** | `5433` | `localhost:5433` | Database Server (Access via pgAdmin) |
| **Redis** | `6379` | `localhost:6379` | In-Memory Cache |
| **Kafka Broker** | `9092` | `localhost:9092` | Message Broker (External Host Listener) |
| **MinIO API** | `9000` | `localhost:9000` | Object Storage API |
| **MinIO Console** | `9001` | `http://localhost:9001` | MinIO Web Management Dashboard |
| **Kafka UI** | `7080` | `http://localhost:7080` | Kafka Topics & Messages Dashboard |
| **Redis Commander** | `7070` | `http://localhost:7070` | Redis Cache Visual Dashboard |

Access the MinIO console at `http://127.0.0.1:9001` (Username: `admin`, Password: `super_secret_password`).
Create a new bucket named `reports` before running the Spark job.

### 2. Backend API Execution

Install the Python dependencies and run the FastAPI server.

```bash
poetry install
poetry run uvicorn app.main:app --reload
```

The API will be available at `http://127.0.0.1:8000`. You can access the Swagger UI documentation at `http://127.0.0.1:8000/docs`.

**API Endpoints:**

* `POST /cities/`: Adds or updates a city and its country code in the PostgreSQL database.

* `GET /cities/{city_name}`: Retrieves the country code. Implements a caching mechanism using Redis. If a cache miss occurs, data is fetched from PostgreSQL. Cache metrics and response times are logged asynchronously to a Kafka topic.

### 3. Big Data Processing (Spark Streaming)

The Spark job reads a continuous stream of SMS logs, joins them with a static reference file, calculates revenues, and aggregates the data using time windows.

Ensure your data files are correctly placed in the `data/REF_SMS/` and `data/ref/` directories. Then, run the PySpark script:

```bash
poetry run python scripts/spark_job.py
```

**Spark Job Operations:**

1. **Ingestion:** Reads CSV streams dynamically from the configured input directory.

2. **Transformation:** Converts monetary units to standard formats and parses timestamps into native Spark types.

3. **Aggregation 1:** Calculates total daily revenue using a 24-hour time window.

4. **Aggregation 2-4:** Calculates total revenue, maximum revenue, minimum revenue, and record count within 15-minute windows, grouped by Payment Type.

5. **Output:** Writes the final aggregated data directly into the local MinIO `reports` bucket as structured CSV files.

Note for Windows Users: The script automatically handles downloading necessary Hadoop binaries (`winutils.exe` and `hadoop.dll`) into a local directory and bypasses S3 connection timeout limitations inherent in `hadoop-aws` 3.3.4.

## Graceful Shutdown

To stop the streaming process, use `Ctrl+C` in the Spark terminal. To shut down the infrastructure and remove the containers:

```bash
docker-compose down
```

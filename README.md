# CpaSync
A tool to calculate Cost Per Acquisition (CPA) from JSON files (fb_spend.json and network_conv.json) and store results in PostgreSQL. It processes data daily via APScheduler, logs operations, and provides a summary of processed records and average CPA.

## Features

Reads JSON data with spend and conversions.
Calculates CPA (spend / conversions) or sets NULL if conversions or spend is 0.
Uses outer join to include all campaigns, even without spend.
Stores results in PostgreSQL with upsert to avoid duplicates.
Runs daily via APScheduler with retry logic for database operations.
Logs to app.log and prints summary to console.
Dockerized for easy deployment.
Linting with ruff via pre-commit.

## Prerequisites

Python 3.12+ (for local setup)
Docker and Docker Compose (for Docker setup)
PostgreSQL 17 (included in Docker setup or installed locally)

## Project Structure
```
CpaSync/
├── .env                  # Environment variables (DB_URL, JSON paths)
├── .env.db              # PostgreSQL credentials
├── app.log              # Logs
├── Dockerfile           # Docker image for app
├── docker-compose.yml   # Docker setup for app and PostgreSQL
├── requirements.txt     # Python dependencies
├── run.py              # Entry point
├── src/                # Core logic
│   ├── data_reader.py   # Reads and merges JSON
│   ├── cpa_calculator.py # Calculates CPA
│   ├── db_repository.py # Database operations
│   ├── scheduler.py     # APScheduler setup
├── tests/              # Unit tests
│   ├── test_cpa.py     # Tests for CPA and data processing
├── data/               # Input JSON files
│   ├── fb_spend.json
│   ├── network_conv.json
```
## Setup and Run
### Option 1: Docker (Recommended, ~3 minutes)
1. **Clone the repository:**
```bash
git clone git@github.com:igor20192/CpaSync.git
cd CpaSync
```

2. **Ensure** `.env` **and** `.env.db` **are configured:**
```bash
#.env
DB_URL=postgresql://user:password@postgres:5432/dbname
SPEND_PATH=data/fb_spend.json
CONV_PATH=data/network_conv.json
```
```bash
# .env.db
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_DB=dbname
```

3. **Build and run with Docker Compose:**
```bash
docker-compose up --build
```

4. **Check logs in `app.log`app.log or console output:**
```bash
Processed 2025-06-04: 2 records
Processed 2025-06-05: 3 records
Processed 2025-06-06: 2 records
Summary: Processed 7 records, Average CPA: 4.51
```

5. **Verify data in PostgreSQL:**
```bash
docker-compose exec postgres psql -U user -d dbname -c "SELECT * FROM daily_stats;"
```



### Option 2: Local Setup

1. **Clone the repository:**
```bash
git clone git@github.com:igor20192/CpaSync.git
cd CpaSync
```

2. **Create and activate a virtual environment:**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```


3. **Install dependencies:**
```bash
pip install -r requirements.txt
```


4. **Set up .env for local PostgreSQL:**
```bash
#.env
DB_URL=postgresql://user:password@localhost:5432/dbname
SPEND_PATH=data/fb_spend.json
CONV_PATH=data/network_conv.json
```
**Ensure PostgreSQL is running locally with the same `user`, `password`, and `dbname`.**

5. **Install pre-commit hooks for linting:**
```bash
pip install pre-commit
pre-commit install
```

6. **Run the application:**
```bash
python run.py --start-date 2025-06-04 --end-date 2025-06-06
```


7. **Check `app.log` and console output (same as Docker).**

**Verify data in PostgreSQL:**
```bash
psql -U user -d dbname -c "SELECT * FROM daily_stats;"
```



## Expected Database Output
```sql
        date         | campaign_id | spend | conversions |        cpa         
---------------------+-------------+-------+-------------+--------------------
 2025-06-04 00:00:00 | CAMP-123    |  37.5 |          14 | 2.6785714285714284
 2025-06-04 00:00:00 | CAMP-456    |  19.9 |           3 | 6.633333333333333
 2025-06-05 00:00:00 | CAMP-123    |  42.1 |          10 | 4.21
 2025-06-05 00:00:00 | CAMP-456    |     0 |           5 | NULL
 2025-06-05 00:00:00 | CAMP-789    |    11 |           0 | NULL
 2025-06-06 00:00:00 | CAMP-888    |     0 |           7 | NULL
 2025-06-06 00:00:00 | CAMP-999    |  5.25 |           0 | NULL
(7 rows)
```
## Running Tests

1. **Ensure dependencies are installed:**
```bash
pip install -r requirements.txt
```


2. **Run unit tests with coverage:**
```bash
pytest tests/ -v --cov=src
```


3. **View coverage report:**
```bash
pytest --cov=src --cov-report=html
```

**Open `htmlcov/index.html` to check coverage (>50%).**

## Code Quality

- **Linting:** Use `ruff` for style checks and formatting:
```bash
ruff check . --fix
ruff format .
```


- **Pre-commit:** Automatically runs `ruff` and `ruff-format` on commit:
```bash
pre-commit run --all-files
```



## 📝Improvements 

- Add integration tests with real SQLite database.
- Implement retry logic for JSON file reading (e.g., for network issues).
- Add API client for fetching data instead of static JSON files.
- Optimize large JSON processing with `pandas.read_json(chunksize=...)`.

## Troubleshooting

- **Database connection error:** Verify `DB_URL` in `.env` matches `.env.db` credentials.
- **No data processed:** Check JSON file paths in `.env` and ensure files exist.
- **Scheduler not running:** Ensure run.py is running continuously (use Docker or `Event().wait()` for local).


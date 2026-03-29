# Python PostgreSQL Banking Lab

A banking system built with Python and PostgreSQL, covering SQL fundamentals through a REST API.

## Stack

- Python 3.12
- PostgreSQL
- psycopg2
- FastAPI
- ReportLab
- pytest

## Setup

1. Clone the repository
2. Create and activate a virtual environment:
    ```bash
    python -m venv .venv
    .venv\Scripts\activate  # Windows
    source .venv/bin/activate  # Linux/Mac
    ```
3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4. Copy `.env.example` to `.env` and fill in your PostgreSQL credentials:
    ```bash
    cp .env.example .env
    ```
5. Create the database and run the schema:
    ```bash
    psql -U postgres -c "CREATE DATABASE banking_lab;"
    psql -U postgres -d banking_lab -f src/schema.sql
    ```

## Running the API

```bash
uvicorn src.api:app --reload
```

Access the interactive docs at `http://localhost:8000/docs`.

## Running the tests

```bash
python -m pytest -v
```

## Project structure

- `src/`
  - `database.py` - Database connection
  - `schema.sql` - DDL operations
  - `operations.py` - Deposit, withdraw, transfer
  - `queries.py` - Account queries
  - `reports.py` - Window function reports
  - `exporters.py` - CSV and PDF export
  - `api.py` - REST API
- `tests/`
  - `test_schema.py`
  - `test_operations.py`
  - `test_queries.py`
  - `test_reports.py`
  - `test_exporters.py`
  - `test_api.py`
# UK Carbon Intensity ETL Pipeline

## Overview
This repository contains an automated Data Engineering ETL (Extract, Transform, Load) pipeline built with **Apache Airflow 3**, **Docker**, and **PostgreSQL**. 

The pipeline automatically monitors the UK National Grid's carbon footprint by ingesting live data from the official Carbon Intensity API, structuring the raw JSON, and loading it into a normalized relational database for downstream analytics and sustainability reporting.

> [!NOTE]
> The UK Carbon Intensity API is public and **does not require an API key** or registration to use.

---

## 🏗 System Architecture

The project operates entirely within Docker containers to ensure clean, reproducible environments.

`[UK Carbon Intensity API] ──> [Airflow 3 (Docker)] ──> [PostgreSQL DB (Docker Port 5433)]`

*   **Extract:** Fetches half-hourly grid metrics (forecast vs. actual carbon intensity).
*   **Transform:** Flattens nested JSON responses into a structured format.
*   **Load:** Inserts data into PostgreSQL. Designed to be **idempotent** (using `ON CONFLICT` constraints) to prevent duplicate records if a task is rerun.

---

## ⚙️ Prerequisites
To run this project locally, you must have the following installed on your machine:
*   **Git**
*   **Docker Desktop** (Make sure the Docker engine is running)
*   **PowerShell** or Terminal
*   A Database Client (e.g., DBeaver, pgAdmin, or VS Code SQLTools)
*   **Python 3.8+** (optional, for local development/autocomplete support)

---

## 🚀 Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/carbon-etl-project.git
cd carbon-etl-project
```

### 2. Configure the Environment
Set up your local user ID so Airflow has the correct file permissions:
```bash
# For Windows PowerShell:
Add-Content -Path .env -Value "AIRFLOW_UID=50000"

# For Mac/Linux:
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 3. Launch the Docker Containers
Initialize the Airflow database, then start all services in the background:
```bash
docker compose up airflow-init
docker compose up -d
```
*Note: The initial boot may take 2-3 minutes as the Airflow 3 webserver starts up.*

If you need to check container logs at any point, run:
```bash
docker compose logs -f
```

### 4. Configure the Database Connection in Airflow
Airflow needs credentials to securely send data to the PostgreSQL container.
1. Navigate to **http://127.0.0.1:8080** in your browser.
2. Log in using `airflow` for both the username and password.
3. Go to **Admin > Connections > + (Add a new record)**.
4. Fill in the following exact details:
   * **Connection Id:** `postgres_target`
   * **Connection Type:** `Postgres`
   * **Host:** `target-db` *(Note: Since Airflow runs inside the Docker network, we connect via service name)*
   * **Database:** `carbon_data`
   * **Login:** `admin`
   * **Password:** `admin` *(Note: Matches the password configured in docker-compose.yaml)*
   * **Port:** `5432`
5. Save the connection.

### 5. Initialize the Target Database Table
Before running the pipelines, you need to create the destination table in the `target-db` database:

1. Connect to the target PostgreSQL container using your database client:
   * **Host:** `localhost` (or `127.0.0.1`)
   * **Port:** `5433` *(Note: Port 5433 maps to the target-db container, while Airflow's metadata DB is on 5432)*
   * **Database:** `carbon_data`
   * **Username:** `admin`
   * **Password:** `admin`
2. Run the following DDL query to create the `national_grid_intensity` table:
   ```sql
   CREATE TABLE IF NOT EXISTS national_grid_intensity (
       record_timestamp TIMESTAMP WITH TIME ZONE PRIMARY KEY,
       forecast_intensity INTEGER,
       actual_intensity INTEGER,
       index_level VARCHAR(50)
   );
   ```

---

## 📖 User Guide

### Running the Pipelines
Once logged into the Airflow UI, you will see two DAGs available:

1.  **[`uk_carbon_historical_backfill`](dags/Backfill.py) (Run Once):**
    *   **Purpose:** Populates the database with the last 14 days of historical carbon data. 
    *   **Action:** Click the "Play" (Trigger) button to run this manually. You only ever need to run this once.
2.  **[`uk_carbon_intensity_etl`](dags/uk_carbon_etl.py) (Automated Daily):**
    *   **Purpose:** The primary pipeline. It runs automatically every day at midnight to fetch the previous 24 hours (48 half-hour blocks) of data.
    *   **Action:** Click the toggle on the left side of the screen to "Unpause" the DAG. It will handle the rest automatically.

### Viewing the Data
To view the transformed data, open your database client and connect to the local target database:
*   **Host:** `localhost` (or `127.0.0.1`)
*   **Port:** `5433`
*   **Database:** `carbon_data`
*   **Username:** `admin`
*   **Password:** `admin`

Run the following SQL query to verify the data:
```sql
SELECT * FROM national_grid_intensity ORDER BY record_timestamp DESC;
```

#### Database Schema Details
The `national_grid_intensity` table consists of the following columns:
*   `record_timestamp` (TIMESTAMP WITH TIME ZONE, PRIMARY KEY): The start time of the half-hour carbon intensity reading.
*   `forecast_intensity` (INTEGER): The predicted carbon intensity in gCO2/kWh.
*   `actual_intensity` (INTEGER): The actual measured carbon intensity in gCO2/kWh.
*   `index_level` (VARCHAR): The categorical rating of carbon intensity (e.g. `very low`, `low`, `moderate`, `high`, `very high`).

---

## 🛑 Stopping & Resetting the Services

*   **Stop the pipeline**: To stop the containers without deleting stored database records:
    ```bash
    docker compose down
    ```
*   **Reset the pipeline (Destroy Data)**: To stop all containers and delete all associated volumes (Airflow metastore and target database data):
    ```bash
    docker compose down -v
    ```

---

## 💻 Local Development (Optional)

To set up a local virtual environment for code completion and linting in your IDE:

1. Create a virtual environment:
   ```bash
   python -m venv .venv
   ```
2. Activate the virtual environment:
   * **Windows (PowerShell)**: `.venv\Scripts\Activate.ps1`
   * **Linux/macOS**: `source .venv/bin/activate`
3. Install development dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Adding Custom Python Packages to Docker
If you need to add custom Python packages to the Airflow Docker containers (e.g., for new DAG tasks) without building a new image, add them to the `_PIP_ADDITIONAL_REQUIREMENTS` variable in your `.env` file:
```env
_PIP_ADDITIONAL_REQUIREMENTS="pandas openpyxl"
```
Then restart the containers to apply:
```bash
docker compose up -d
```

---

## 🤝 Contribution Guidelines

Future developers looking to expand this project are welcome to contribute! Here is how to add new features safely:

1.  **Adding New Data Sources:** If integrating a new API endpoint (e.g., regional breakdown data), create a separate Python file in the `/dags` directory rather than overloading the current DAG. Use the `@dag` and `@task` TaskFlow decorators.
2.  **Database Migrations:** If adding new columns to the PostgreSQL schema, ensure you update the target database definition as well as any DDL blocks or references.
3.  **Idempotency:** Any new `INSERT` operations must include an `ON CONFLICT` clause (or equivalent upsert logic) to maintain idempotency and ensure data integrity upon reruns.
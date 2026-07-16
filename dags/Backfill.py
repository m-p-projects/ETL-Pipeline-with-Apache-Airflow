import requests
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "milan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="uk_carbon_historical_backfill",
    default_args=default_args,
    start_date=datetime(2026, 7, 8),
    schedule=None,  # Setting schedule to None makes it a "Run Manually Only" DAG
    catchup=False,
    description="A one-off backfill pipeline for 14 days of historical Carbon data",
)
def carbon_historical_pipeline():

    @task
    def extract_historical_data():
        """Fetches the last 14 days of data using the date-range API endpoint."""
        # Calculate exactly 14 days ago from right now
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=14)

        # The API requires dates in a specific ISO8601 string format (e.g., 2026-06-25T00:00Z)
        from_str = start_date.strftime("%Y-%m-%dT%H:%MZ")
        to_str = end_date.strftime("%Y-%m-%dT%H:%MZ")

        url = f"https://api.carbonintensity.org.uk/intensity/{from_str}/{to_str}"
        response = requests.get(url)
        response.raise_for_status()

        return response.json()

    @task
    def transform_historical_data(raw_data):
        """Loops through hundreds of historical records and flattens them."""
        transformed_list = []

        # Instead of just taking the first block, we loop through the entire list
        for block in raw_data["data"]:
            transformed_list.append(
                {
                    "timestamp": block["from"],
                    "forecast_intensity": block["intensity"]["forecast"],
                    "actual_intensity": block["intensity"]["actual"],
                    "index_level": block["intensity"]["index"],
                }
            )

        return transformed_list

    @task
    def load_historical_data(clean_data):
        """Loads the array of historical data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id="postgres_target")

        insert_sql = """
        INSERT INTO national_grid_intensity (record_timestamp, forecast_intensity, actual_intensity, index_level)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (record_timestamp) DO NOTHING;
        """

        # Loop through our clean list and insert each row
        # (The ON CONFLICT clause ensures we don't accidentally duplicate rows)
        for row in clean_data:
            pg_hook.run(
                insert_sql,
                parameters=(
                    row["timestamp"],
                    row["forecast_intensity"],
                    row["actual_intensity"],
                    row["index_level"],
                ),
            )

        print(f"Successfully loaded {len(clean_data)} historical rows!")

    # Task Flow
    raw = extract_historical_data()
    clean = transform_historical_data(raw)
    load_historical_data(clean)


carbon_historical_pipeline()

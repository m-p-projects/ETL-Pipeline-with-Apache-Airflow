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
    dag_id="uk_carbon_intensity_etl",
    default_args=default_args,
    start_date=datetime(2026, 7, 8),
    schedule="@daily",
    catchup=False,
    description="Daily ETL pipeline for UK Carbon Intensity data",
)
def carbon_etl_pipeline():

    @task
    def extract_carbon_data():
        """Fetches all 48 half-hour blocks for yesterday."""
        # Calculate exactly yesterday's date
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")

        # Use the specific date endpoint to get the full 24 hours of data
        url = f"https://api.carbonintensity.org.uk/intensity/date/{date_str}"
        response = requests.get(url)
        response.raise_for_status()

        return response.json()

    @task
    def transform_carbon_data(raw_data):
        """Loops through the 48 blocks and flattens them."""
        transformed_list = []

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
    def load_carbon_data(clean_data):
        """Loads the array of yesterday's data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id="postgres_target")

        insert_sql = """
        INSERT INTO national_grid_intensity (record_timestamp, forecast_intensity, actual_intensity, index_level)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (record_timestamp) DO NOTHING;
        """

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

    # Task Flow
    raw = extract_carbon_data()
    clean = transform_carbon_data(raw)
    load_carbon_data(clean)


carbon_etl_pipeline()

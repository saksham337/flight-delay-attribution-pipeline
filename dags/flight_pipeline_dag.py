"""
Flight Delay Attribution Pipeline - Airflow DAG

STATUS: Designed but not deployed in this proof-of-concept environment.

This DAG encodes the same pipeline steps executed manually as SQL in
scripts/pipeline.sql. Orchestration via Airflow was scoped out for this
proof of concept after the shared Snowflake environment's MFA and network
policy restrictions blocked programmatic (non-interactive) authentication
required for scheduled/automated connections. Manual execution in Snowsight
was used instead to keep the analytical timeline on track.

This file demonstrates the intended production orchestration: what would
run, in what order, and on what schedule, once a Snowflake service account
with programmatic access is provisioned.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

DEFAULT_ARGS = {
    "owner": "saksham",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_delay_attribution_pipeline",
    description="Cross-references airline delay attribution against observed weather",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["flight-delays", "weather", "attribution"],
) as dag:

    # --------------------------------------------------------------
    # 1. Raw ingestion (currently done manually via Snowsight upload
    #    wizard: flights_2015_01.csv, airlines.csv, airports.csv,
    #    5x weather CSV.gz files -> RAW schema)
    # --------------------------------------------------------------
    # In production this step would be a set of tasks pulling from
    # BTS/Kaggle and the Meteostat API directly into Snowflake stages,
    # then COPY INTO the raw tables. Omitted here since ingestion was
    # done manually for this proof of concept.

    # --------------------------------------------------------------
    # 2. Build dimensions
    # --------------------------------------------------------------
    build_dim_airport = SnowflakeOperator(
        task_id="build_dim_airport",
        snowflake_conn_id="snowflake_default",
        sql="sql/01_dim_airport.sql",
    )

    build_dim_carrier = SnowflakeOperator(
        task_id="build_dim_carrier",
        snowflake_conn_id="snowflake_default",
        sql="sql/02_dim_carrier.sql",
    )

    # --------------------------------------------------------------
    # 3. Build staging layer
    # --------------------------------------------------------------
    build_stg_flights = SnowflakeOperator(
        task_id="build_stg_flights",
        snowflake_conn_id="snowflake_default",
        sql="sql/03_stg_flights.sql",
    )

    # --------------------------------------------------------------
    # 4. Build fact tables
    # --------------------------------------------------------------
    build_fct_flight = SnowflakeOperator(
        task_id="build_fct_flight",
        snowflake_conn_id="snowflake_default",
        sql="sql/04_fct_flight.sql",
    )

    build_fct_weather_observation = SnowflakeOperator(
        task_id="build_fct_weather_observation",
        snowflake_conn_id="snowflake_default",
        sql="sql/05_fct_weather_observation.sql",
    )

    # --------------------------------------------------------------
    # 5. Build the analytical mart
    # --------------------------------------------------------------
    build_mart_flight_weather = SnowflakeOperator(
        task_id="build_mart_flight_weather",
        snowflake_conn_id="snowflake_default",
        sql="sql/06_mart_flight_weather.sql",
    )

    # --------------------------------------------------------------
    # 6. Data quality checks
    # --------------------------------------------------------------
    run_data_quality_checks = SnowflakeOperator(
        task_id="run_data_quality_checks",
        snowflake_conn_id="snowflake_default",
        sql="sql/07_data_quality_checks.sql",
    )

    # --------------------------------------------------------------
    # Task dependencies
    # --------------------------------------------------------------
    [build_dim_airport, build_dim_carrier] >> build_stg_flights
    build_stg_flights >> build_fct_flight
    build_fct_flight >> build_fct_weather_observation
    build_fct_weather_observation >> build_mart_flight_weather
    build_mart_flight_weather >> run_data_quality_checks

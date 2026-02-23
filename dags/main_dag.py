from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys

sys.path.insert(0, "/opt/airflow/src")

from extract import fetch_api
from transform import transform_cafe_table, transform_menu_items_table, transform_orders_table, transform_orders_items_table
from load import append_data, truncate_data

API_URL = "http://127.0.0.1:5000"

@dag(
    dag_id="cafe_analises",
    start_date=datetime(2026, 2, 18),
    schedule="0 */1 * * *",
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past":False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["pipeline", "cafe"]
)
def pipeline():

    from pyspark.sql import SparkSession
    
    def get_spark():
        return (
            SparkSession.builder
            .appName("cafe_pipeline")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .getOrCreate()
        )
    
    #=========
    # Extract
    #=========
    @task
    def extract():
        fetch_api(API_URL)

    #=======================
    # Transform + Load cafe
    #=======================
    @task
    def process_cafe():
        spark = get_spark()
        # Transform
        df = transform_cafe_table(spark, "/opt/airflow/data/cafes.csv")

        # Load
        truncate_data("cafes_table", df)
    
    #===========================
    # Transform + Load menu item
    #===========================
    @task
    def process_menu():
        spark = get_spark()
        # Transform
        df = transform_menu_items_table(spark, "/opt/airflow/data/menu_items.csv")

        # Load
        truncate_data("menu_items_table", df)
    
    #========================
    # Transform + Load Orders
    #========================
    @task
    def process_orders():
        spark = get_spark()

        # Transform
        df = transform_orders_table(spark, "/opt/airflow/data/orders_data.json")

        # Load
        append_data("orders_table", df)

    #==============================
    # Transform + Load Orders Items
    #==============================
    @task
    def process_order_items():
        spark = get_spark()

        # Transform
        df = transform_orders_items_table(spark, "/opt/airflow/data/orders_items_data.json")

        # Load
        append_data("orders_items_table", df)

    e = extract()

    cafe = process_cafe()
    menu = process_menu()
    orders = process_orders()
    items = process_order_items()

    e >> [cafe, menu, orders, items]

pipeline()
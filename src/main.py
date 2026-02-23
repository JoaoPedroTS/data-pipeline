from extract import fetch_api
from transform import transform_cafe_table, transform_menu_items_table, transform_orders_table, transform_orders_items_table
from load import append_data, truncate_data

from pyspark.sql import SparkSession
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder
    .appName("pipeline")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate()
)

API_URL = "http://127.0.0.1:5000"

def pipeline():
    
    try:
        fetch_api(API_URL)
        
        cafes_df = transform_cafe_table(spark, "data/cafes.csv")
        logger.info("Transformacão da tabela `cafe` finalizada")
        
        logger.info("Carregando tabela `cafe`")
        truncate_data("cafes_table", cafes_df)
        
        menu_items_df = transform_menu_items_table(spark, "data/menu_items.csv")
        logger.info("Transformacão da tabela `menu_item` finalizada")
        
        logger.info("Carregando tabela `menu_items`")
        truncate_data("menu_items", menu_items_df)
        
        orders_df = transform_orders_table(spark, "data/orders_data.json")
        logger.info("Transformacão da tabela `sales_orders` finalizada")

        logger.info("Carregando tabela `orders_table`")
        append_data("orders_table", orders_df)
        
        orders_items_df = transform_orders_items_table(spark, "data/orders_items_data.json")
        logger.info("Transformacão da tabela `orders_items` finalizada")

        logger.info("Carregando tabela `orders_items_table`")
        append_data("orders_items_table", orders_items_df)

        logger.info("Pipeline finalizada com sucesso")
    
    except Exception as e:
        logging.error(f" ERRO na pipeline: {e}")

        import traceback
        traceback.print_exc()

pipeline()
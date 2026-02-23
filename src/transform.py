from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract, regexp_replace, col, round, trim, hour, date_format, when
from pyspark.sql.types import IntegerType, BooleanType
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

#============================
#Tabela cafe
#============================
def seats_column(df: DataFrame) -> DataFrame:
    '''
    Desmembrar colunas `seats` em `min_seats` e `max_seats`
    '''
    
    # Normalizar
    df = df.withColumn(
        "seats",
        regexp_replace(col("seats"), r"\s*-\s*", "-")
    ).withColumn(
        "seats",
        regexp_replace(col("seats"), r"\+$", "-50")
    )

    # Cria colunas
    min_col = regexp_extract(col("seats"), r"^(\d+)", 1)
    max_col = regexp_extract(col("seats"), r"-(\d+)$", 1)

    df = df.withColumn("min_seats", min_col.cast(IntegerType()))
    df = df.withColumn(
        "max_seats",
        when(max_col != "", max_col.cast(IntegerType()))
        .otherwise(col("min_seats"))
    )
    df = df.withColumn(
        "avg_seats",
        ((col("min_seats") + col("max_seats")) / 2).cast(IntegerType())
    )

    return df

def convert_columns_to_binary(df: DataFrame, columns_names: list[str]) -> DataFrame:
    '''
    Converte colunas para o tipo booleano
    '''

    existing_cols = set(df.columns)

    for c in columns_names:
        if c in existing_cols:
            df = df.withColumn(c, col(c).cast(BooleanType()))

    return df

def trim_text_columns(df: DataFrame, columns_names: list[str]) -> DataFrame:
    '''
    Remove espaços no início e fim de colunas string
    '''
    
    existing_cols = set(df.columns)
    
    for c in columns_names:
        if c in existing_cols:
            df = df.withColumn(c, trim(col(c)))
    
    return df

def establishment_type_column(df: DataFrame) -> DataFrame:
    '''
    criar coluna categorica establishment_type
    '''
    
    df = df.withColumn(
        "establishment_type",
        when(col("avg_seats") < 10, "Coffee Stand / To Go")
        .when(
            col("has_wifi") & col("has_sockets") & (col("avg_seats") > 40),
            "Co-working Friendly"
        )
        .when(col("avg_seats") > 40, "Large Cafe")
        .otherwise("Standard Cafe")
    )

    return df

def transform_cafe_table(spark, file_path: str) -> DataFrame:
    df = spark.read.option("header", True).csv(file_path)
    
    df = seats_column(df)
    logger.info("Transformacão da coluna `seats` concluida")
    
    cols_to_convert = ["has_sockets", "has_toilet", "has_wifi", "can_take_calls"]
    df = convert_columns_to_binary(df=df, columns_names=cols_to_convert)
    logger.info("Tranformacão de colunas binarias concluida")

    cols_to_trim = ["name", "location"]
    df = trim_text_columns(df=df, columns_names=cols_to_trim)
    logger.info("Tranformacão das colunas de texto concluida")

    df = establishment_type_column(df)
    logger.info("Coluna `establishment_type criada`")

    return df

#============================
#Tabela menu item
#============================
def profit_columns(df: DataFrame) -> DataFrame:
    '''
    calcular gross margin e margin percentage
    '''

    df = df.withColumn("price", col("price").cast("double")).withColumn("cost", col("cost").cast("double"))
    
    df = df.withColumn(
        "gross_margin",
        round(col("price") - col("cost"), 2)  # Margem Bruta
    ).withColumn(
        "margin_percentage",
        when(
            col("price") != 0,
            round(((col("price") - col("cost")) / col("price")) * 100, 2)
        ).otherwise(None)  # % Margem
    )

    return df

def price_range_column(df: DataFrame):
    '''
    criar coluna categorica
    '''
    df = df.withColumn(
        "price_category",
        when(col("price") < 10, "Budget")
        .when((col("price") >= 10) & (col("price") <= 20), "Standard")
        .otherwise("Premium")
    )

    return df

def transform_menu_items_table(spark, file_path: str) -> DataFrame:
    df = spark.read.option("header", True).csv(file_path)

    cols_to_trim = ["name", "category"]
    df = trim_text_columns(df, cols_to_trim)
    
    df = profit_columns(df)
    
    df = price_range_column(df)

    return df

#============================
#Tabela sales orders 
#============================
def timestamp_column(df: DataFrame):
    '''
    Destrinchar coluna timestamp
    '''

    df = df.withColumn(
        "created_at",
        F.to_timestamp(col("created_at"))
    )
    
    df = df.withColumn(
        "order_date",
        date_format(col("created_at"), "yyyy-MM-dd")  # Apenas a data
    ).withColumn(
        "order_hour",
        hour(col("created_at"))  # Hora 0-23
    ).withColumn(
        "day_of_week",
        date_format(col("created_at"), "EEEE")  # Segunda, Terça...
    ).withColumn(
        "month_name",
        date_format(col("created_at"), "MMMM")  # Janeiro, Fevereiro...
    )

    df = df.withColumn(
        "day_period",
        when((col("order_hour") >= 6) & (col("order_hour") <= 11), "Manhã")
        .when((col("order_hour") >= 12) & (col("order_hour") <= 14), "Almoço")
        .when((col("order_hour") >= 15) & (col("order_hour") <= 18), "Tarde")
        .when(col("order_hour") >= 19, "Noite")
        .otherwise("Fora do horário")  # cobre 0-5
    )

    return df

def transform_orders_table(spark, file_path: str):
    df = spark.read.option("multiline", True).json(file_path)
    
    cols_to_trim = ["payment_method"]
    df = trim_text_columns(df, cols_to_trim)

    df = timestamp_column(df)

    return df

#============================
#Tabela sales orders items
#============================
def total_price_column(df: DataFrame) -> DataFrame:
    '''
    Calcular preco total
    '''
    
    df = df.withColumn(
        "total_price",
        round(col("quantity") * col("unit_price"), 2)  
    )

    return df

def transform_orders_items_table(spark, file_path: str):
    df = spark.read.option("multiline", True).json(file_path)
    
    df = total_price_column(df)

    return df
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent/ "config" / ".env"

load_dotenv(env_path)

user = os.getenv("DATABASE_USER")
password = os.getenv("DATABASE_PASSWORD")
database = os.getenv("DATABASE")
host = "127.0.0.1"

if not all([user, password, database]):
    raise ValueError("Variáveis de ambiente do banco não definidas")

jdbc_url = f"jdbc:postgresql://{host}:5432/{database}"
properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

def append_data(table_name: str, df):
    (
        df.write
        .mode("append")
        .jdbc(
            url=jdbc_url,
            table=table_name,
            properties=properties
        )
    )

def truncate_data(table_name: str, df):
    (
        df.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(
            url=jdbc_url,
            table=table_name,
            properties=properties
        )
    )

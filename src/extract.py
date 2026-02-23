import requests
import json
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def save_json(path: str, content: dict, logger) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(content, f, indent=2)

    logger.info(f"Arquivo salvo em: {output_path}")

def fetch_api(url:str) -> dict:
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            logger.warning("Nenhum dado retornado")
            return {}

        orders_output_path = "data/orders_data.json"
        orders_data = data.get('order')

        logger.info(f"Venda: {orders_data}")

        save_json(orders_output_path, orders_data, logger)

        orders_items_output_path = "data/orders_items_data.json"
        orders_items_data = data.get("items", [])

        logger.info("Itens da venda:")
        for item in orders_items_data:
            logger.info(item)

        save_json(orders_items_output_path, orders_items_data, logger)
        
        return data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro inesperado: {e}")

        return {}
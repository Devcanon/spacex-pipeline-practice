import requests
import json
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def fetch_data():
    urls = {
        "launches": "https://api.spacexdata.com/v4/launches",
        "rockets": "https://api.spacexdata.com/v4/rockets",
        "payloads": "https://api.spacexdata.com/v4/payloads"
    }

    results = {}

    for name, url in urls.items():
        try:
            response = requests.get(url)
            logger.info(f"Попытка отправить запрос [{name}]. . .")
            if response.status_code == 200:
                logger.info(f"Успешно. Код ответа [{name}]: {response.status_code}")
                results[name] = response.json()
            else:
                logger.error(f"Ошибка при получении данных [{name}]: {response.status_code}")
                results[name] = None
        except requests.RequestException as e:
            logger.error(f"Ошибка сети при получении данных [{name}]: {e}")
            results[name] = None
    
    return results.get("launches"), results.get("rockets"), results.get("payloads")
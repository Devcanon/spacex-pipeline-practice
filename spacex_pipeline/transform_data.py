import psycopg2
import logging
import pandas as pd
import json
from db.connection import get_connection

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def transformed_fact_data(conn):
    try:
        logger.info("Считывание данных для таблицы фактов...")
        launches_df = pd.read_sql_query("SELECT id, flight_number, name, date_utc, success FROM spacex_data.raw_spacex_launches_data", conn)
        if launches_df.empty:
            logger.warning("Данные из [launches] не получены или пусты.")
            return None
        logger.info(f"Данные из [launches] успешно считаны: {len(launches_df)} записей.")
        
        links_df = pd.read_sql_query("SELECT launch_id, webcast, wikipedia FROM spacex_data.raw_spacex_links_data", conn)
        if links_df.empty:
            logger.warning("Данные из [links] не получены или пусты.")
            return None
        logger.info(f"Данные из [links] успешно считаны: {len(links_df)} записей.")

        try:
            logger.info("Объединение таблиц для fct_launches...")
            fct_df = pd.merge(launches_df, links_df, left_on="id", right_on="launch_id", how="inner")
            fct_df.drop(columns=["launch_id"], inplace=True)
            logger.info("Таблицы успешно объединены.")
            return fct_df
        except Exception as e:  
            logger.error(f"Ошибка при объединении таблиц: {e}")
            return None
    except Exception as e:
        logger.error(f"Ошибка при обработке данных для таблицы фактов: {e}")
        return None
        
def transformed_dimension_data(conn):
    try:
        logger.info("Считывание данных для таблиц измерений...")
        rockets_df = pd.read_sql_query("SELECT id, cost_per_launch, first_flight, height, diameter, mass, payload_weights FROM spacex_data.raw_spacex_rockets_data", conn)
        if rockets_df.empty:
            logger.warning("Данные из [rockets] не получены или пусты.")
            return None, None
        logger.info(f"Данные из [rockets] успешно считаны: {len(rockets_df)} записей.")

        payloads_df = pd.read_sql_query("SELECT id, name, type, mass_kg, orbit FROM spacex_data.raw_spacex_payloads_data", conn)
        if payloads_df.empty:
            logger.warning("Данные из [payloads] не получены или пусты.")
            return rockets_df, None
        logger.info(f"Данные из [payloads] успешно считаны: {len(payloads_df)} записей.")
        
        return rockets_df, payloads_df
            
    except Exception as e:
        logger.error(f"Ошибка при считывании данных для таблиц измерений: {e}")
        return None, None
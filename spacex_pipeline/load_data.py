import logging
import pandas as pd
import psycopg2
import json # <--- 1. ДОБАВЛЕН ИМПОРТ
from psycopg2.extras import execute_values
from db.connection import get_connection
from transform_data import transformed_fact_data, transformed_dimension_data

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def create_transformed_tables(conn):
    with conn.cursor() as cur:
        try:
            cur.execute("CREATE SCHEMA IF NOT EXISTS spacex_analytics;")
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS spacex_analytics.fct_launches (
                    id TEXT PRIMARY KEY, flight_number INTEGER, name TEXT, date_utc TIMESTAMP,
                    success BOOLEAN, webcast TEXT, wikipedia TEXT
                );
                        
                CREATE TABLE IF NOT EXISTS spacex_analytics.dim_rockets (
                    id TEXT PRIMARY KEY, cost_per_launch NUMERIC, first_flight DATE,
                    height JSONB, diameter JSONB, mass JSONB, payload_weights JSONB
                );
                        
                CREATE TABLE IF NOT EXISTS spacex_analytics.dim_payloads (
                    id TEXT PRIMARY KEY, name TEXT, type TEXT, mass_kg NUMERIC, orbit TEXT
                );
                """)
            conn.commit()
            logger.info("Аналитические таблицы готовы к работе в схеме 'spacex_analytics'.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при создании аналитических таблиц: {e}")
            raise

def insert_transformed_data_sql(conn, fct_df, rockets_df, payloads_df):
    with conn.cursor() as cur:
        try:
            if fct_df is not None and not fct_df.empty:
                logger.info(f"Вставка {len(fct_df)} записей в fct_launches...")
                cur.execute("TRUNCATE TABLE spacex_analytics.fct_launches RESTART IDENTITY CASCADE;")
                tuples = [tuple(x) for x in fct_df.to_numpy()]
                cols = ','.join(list(fct_df.columns))
                query = f"INSERT INTO spacex_analytics.fct_launches ({cols}) VALUES %s"
                execute_values(cur, query, tuples)
                logger.info("Данные успешно вставлены в fct_launches.")
            else:
                logger.warning("Нет данных для вставки в fct_launches.")

            if rockets_df is not None and not rockets_df.empty:
                logger.info(f"Вставка {len(rockets_df)} записей в dim_rockets...")
                cur.execute("TRUNCATE TABLE spacex_analytics.dim_rockets RESTART IDENTITY CASCADE;")
                
                for col in ['height', 'diameter', 'mass', 'payload_weights']:
                    if col in rockets_df.columns:
                        rockets_df[col] = rockets_df[col].apply(lambda x: json.dumps(x) if x is not None else None)

                tuples = [tuple(x) for x in rockets_df.to_numpy()]
                cols = ','.join(list(rockets_df.columns))
                query = f"INSERT INTO spacex_analytics.dim_rockets ({cols}) VALUES %s"
                execute_values(cur, query, tuples)
                logger.info("Данные успешно вставлены в dim_rockets.")
            else:
                logger.warning("Нет данных для вставки в dim_rockets.")

            if payloads_df is not None and not payloads_df.empty:
                logger.info(f"Вставка {len(payloads_df)} записей в dim_payloads...")
                cur.execute("TRUNCATE TABLE spacex_analytics.dim_payloads RESTART IDENTITY CASCADE;")
                tuples = [tuple(x) for x in payloads_df.to_numpy()]
                cols = ','.join(list(payloads_df.columns))
                query = f"INSERT INTO spacex_analytics.dim_payloads ({cols}) VALUES %s"
                execute_values(cur, query, tuples)
                logger.info("Данные успешно вставлены в dim_payloads.")
            else:
                logger.warning("Нет данных для вставки в dim_payloads.")

            conn.commit()

        except Exception as e:
            logger.error(f"Ошибка при вставке данных через psycopg2: {e}")
            conn.rollback()
            raise

def main():
    conn = None
    try:
        conn = get_connection()
        if not conn:
            raise ConnectionError("Не удалось получить соединение psycopg2.")

        create_transformed_tables(conn)

        logger.info("Начало трансформации данных...")
        fct_df = transformed_fact_data(conn)
        rockets_df, payloads_df = transformed_dimension_data(conn)
        logger.info("Трансформация данных завершена.")
        
        if fct_df is not None or rockets_df is not None or payloads_df is not None:
            insert_transformed_data_sql(conn, fct_df, rockets_df, payloads_df)
        else:
            logger.error("Не удалось получить все необходимые данные для вставки.")
            
    except Exception as e:
        logger.error(f"Ошибка в главном процессе загрузки: {e}")
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()
import psycopg2
from db.connection import get_connection
from api.get_data import fetch_data
import logging
import json 

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def extract_create_table(conn):
    with conn.cursor() as cur:
        try:
            cur.execute("CREATE SCHEMA IF NOT EXISTS spacex_data;")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_launches_data (
                    id TEXT PRIMARY KEY, flight_number INTEGER, name TEXT, date_utc TIMESTAMP,
                    date_unix INTEGER, date_local TEXT, date_precision TEXT, static_fire_date_utc TIMESTAMP,
                    static_fire_date_unix INTEGER, net BOOLEAN, "window" INTEGER, rocket TEXT,
                    success BOOLEAN, details TEXT, launchpad TEXT, auto_update BOOLEAN,
                    tbd BOOLEAN, launch_library_id TEXT, upcoming BOOLEAN
                );
                
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_fairings_data (
                    launch_id TEXT REFERENCES spacex_data.raw_spacex_launches_data(id) ON DELETE CASCADE,
                    reused BOOLEAN, recovery_attempt BOOLEAN, recovered BOOLEAN
                );
                
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_links_data (
                    launch_id TEXT REFERENCES spacex_data.raw_spacex_launches_data(id) ON DELETE CASCADE,
                    patch_small TEXT, patch_large TEXT, webcast TEXT, youtube_id TEXT, article TEXT,
                    wikipedia TEXT, reddit_campaign TEXT, reddit_launch TEXT, reddit_media TEXT, reddit_recovery TEXT
                );
                        
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_failures_data (
                    launch_id TEXT REFERENCES spacex_data.raw_spacex_launches_data(id) ON DELETE CASCADE,
                    time INTEGER, altitude INTEGER, reason TEXT
                );
                        
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_cores_data (
                    launch_id TEXT REFERENCES spacex_data.raw_spacex_launches_data(id) ON DELETE CASCADE,
                    core TEXT, flight INTEGER, gridfins BOOLEAN, legs BOOLEAN, reused BOOLEAN,
                    landing_attempt BOOLEAN, landing_success BOOLEAN, landing_type TEXT, landpad TEXT
                );
                
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_launch_payloads_data (launch_id TEXT, payload_id TEXT);
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_launch_crew_data (launch_id TEXT, crew_id TEXT);
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_launch_ships_data (launch_id TEXT, ship_id TEXT);
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_launch_capsules_data (launch_id TEXT, capsule_id TEXT);
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_rockets_data (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    type TEXT,
                    active BOOLEAN,
                    stages INTEGER,
                    boosters INTEGER,
                    cost_per_launch BIGINT,
                    success_rate_pct REAL,
                    first_flight DATE,
                    country TEXT,
                    company TEXT,
                    height JSONB,
                    diameter JSONB,
                    mass JSONB,
                    payload_weights JSONB,
                    flickr_images TEXT[],
                    wikipedia TEXT,
                    description TEXT
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS spacex_data.raw_spacex_payloads_data (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    type TEXT,
                    reused BOOLEAN,
                    launch TEXT,
                    customers TEXT[],
                    norad_ids INTEGER[],
                    nationalities TEXT[],
                    manufacturers TEXT[],
                    mass_kg REAL,
                    mass_lbs REAL,
                    orbit TEXT,
                    reference_system TEXT,
                    regime TEXT,
                    longitude REAL,
                    semi_major_axis_km REAL,
                    eccentricity REAL,
                    periapsis_km REAL,
                    apoapsis_km REAL,
                    inclination_deg REAL,
                    period_min REAL,
                    lifespan_years REAL
                );
            """)

            conn.commit()
            logger.info("Все таблицы подготовлены к работе.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при подготовке таблиц: {e}")


def insert_rockets(conn, rockets):
    if not rockets:
        logger.warning("Нет данных о ракетах для вставки.")
        return
    
    logger.info(f"Вставка/обновление {len(rockets)} записей о ракетах...")
    with conn.cursor() as cur:
        for rocket in rockets:
            try:
                cur.execute("""
                    INSERT INTO spacex_data.raw_spacex_rockets_data (
                        id, name, type, active, stages, boosters, cost_per_launch, success_rate_pct,
                        first_flight, country, company, height, diameter, mass, payload_weights,
                        flickr_images, wikipedia, description
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name, type = EXCLUDED.type, active = EXCLUDED.active,
                        stages = EXCLUDED.stages, boosters = EXCLUDED.boosters,
                        cost_per_launch = EXCLUDED.cost_per_launch, success_rate_pct = EXCLUDED.success_rate_pct,
                        first_flight = EXCLUDED.first_flight, country = EXCLUDED.country, company = EXCLUDED.company,
                        height = EXCLUDED.height, diameter = EXCLUDED.diameter, mass = EXCLUDED.mass,
                        payload_weights = EXCLUDED.payload_weights, flickr_images = EXCLUDED.flickr_images,
                        wikipedia = EXCLUDED.wikipedia, description = EXCLUDED.description;
                """, (
                    rocket.get('id'), rocket.get('name'), rocket.get('type'), rocket.get('active'),
                    rocket.get('stages'), rocket.get('boosters'), rocket.get('cost_per_launch'),
                    rocket.get('success_rate_pct'), rocket.get('first_flight'), rocket.get('country'),
                    rocket.get('company'), json.dumps(rocket.get('height')), json.dumps(rocket.get('diameter')),
                    json.dumps(rocket.get('mass')), json.dumps(rocket.get('payload_weights')),
                    rocket.get('flickr_images'), rocket.get('wikipedia'), rocket.get('description')
                ))
            except Exception as e:
                logger.error(f"Ошибка при обработке rocket ID {rocket.get('id')}: {e}")
                conn.rollback()
                continue
        conn.commit()
    logger.info("Данные о ракетах успешно загружены.")


def insert_payloads(conn, payloads):
    if not payloads:
        logger.warning("Нет данных о полезных нагрузках для вставки.")
        return

    logger.info(f"Вставка/обновление {len(payloads)} записей о полезных нагрузках...")
    with conn.cursor() as cur:
        for payload in payloads:
            try:
                cur.execute("""
                    INSERT INTO spacex_data.raw_spacex_payloads_data (
                        id, name, type, reused, launch, customers, norad_ids, nationalities,
                        manufacturers, mass_kg, mass_lbs, orbit, reference_system, regime,
                        longitude, semi_major_axis_km, eccentricity, periapsis_km,
                        apoapsis_km, inclination_deg, period_min, lifespan_years
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        name=EXCLUDED.name, type=EXCLUDED.type, reused=EXCLUDED.reused, launch=EXCLUDED.launch,
                        customers=EXCLUDED.customers, norad_ids=EXCLUDED.norad_ids, nationalities=EXCLUDED.nationalities,
                        manufacturers=EXCLUDED.manufacturers, mass_kg=EXCLUDED.mass_kg, mass_lbs=EXCLUDED.mass_lbs,
                        orbit=EXCLUDED.orbit, reference_system=EXCLUDED.reference_system, regime=EXCLUDED.regime,
                        longitude=EXCLUDED.longitude, semi_major_axis_km=EXCLUDED.semi_major_axis_km,
                        eccentricity=EXCLUDED.eccentricity, periapsis_km=EXCLUDED.periapsis_km,
                        apoapsis_km=EXCLUDED.apoapsis_km, inclination_deg=EXCLUDED.inclination_deg,
                        period_min=EXCLUDED.period_min, lifespan_years=EXCLUDED.lifespan_years;
                """, (
                    payload.get('id'), payload.get('name'), payload.get('type'), payload.get('reused'),
                    payload.get('launch'), payload.get('customers'), payload.get('norad_ids'),
                    payload.get('nationalities'), payload.get('manufacturers'), payload.get('mass_kg'),
                    payload.get('mass_lbs'), payload.get('orbit'), payload.get('reference_system'),
                    payload.get('regime'), payload.get('longitude'), payload.get('semi_major_axis_km'),
                    payload.get('eccentricity'), payload.get('periapsis_km'), payload.get('apoapsis_km'),
                    payload.get('inclination_deg'), payload.get('period_min'), payload.get('lifespan_years')
                ))
            except Exception as e:
                logger.error(f"Ошибка при обработке payload ID {payload.get('id')}: {e}")
                conn.rollback()
                continue
        conn.commit()
    logger.info("Данные о полезных нагрузках успешно загружены.")


def insert_launches(conn, launches):
    if not launches:
        logger.warning("Нет данных о запусках для вставки.")
        return
    
    logger.info(f"Вставка/обновление {len(launches)} записей о запусках...")
    child_tables = [
        "raw_spacex_fairings_data", "raw_spacex_links_data", "raw_spacex_failures_data",
        "raw_spacex_cores_data", "raw_spacex_launch_payloads_data", "raw_spacex_launch_crew_data",
        "raw_spacex_launch_ships_data", "raw_spacex_launch_capsules_data"
    ]

    with conn.cursor() as cur:
        for launch in launches:
            launch_id = launch.get("id")
            if not launch_id:
                continue

            try:
                for table in child_tables:
                    cur.execute(f"DELETE FROM spacex_data.{table} WHERE launch_id = %s;", (launch_id,))

                cur.execute("""
                    INSERT INTO spacex_data.raw_spacex_launches_data (
                        id, flight_number, name, date_utc, date_unix, date_local, date_precision,
                        static_fire_date_utc, static_fire_date_unix, net, "window", rocket,
                        success, details, launchpad, auto_update, tbd, launch_library_id, upcoming
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        flight_number = EXCLUDED.flight_number, name = EXCLUDED.name, date_utc = EXCLUDED.date_utc,
                        date_unix = EXCLUDED.date_unix, date_local = EXCLUDED.date_local,
                        date_precision = EXCLUDED.date_precision, static_fire_date_utc = EXCLUDED.static_fire_date_utc,
                        static_fire_date_unix = EXCLUDED.static_fire_date_unix, net = EXCLUDED.net,
                        "window" = EXCLUDED.window, rocket = EXCLUDED.rocket, success = EXCLUDED.success,
                        details = EXCLUDED.details, launchpad = EXCLUDED.launchpad,
                        auto_update = EXCLUDED.auto_update, tbd = EXCLUDED.tbd,
                        launch_library_id = EXCLUDED.launch_library_id, upcoming = EXCLUDED.upcoming;
                """, (
                    launch_id, launch.get("flight_number"), launch.get("name"),
                    launch.get("date_utc"), launch.get("date_unix"), launch.get("date_local"),
                    launch.get("date_precision"), launch.get("static_fire_date_utc"),
                    launch.get("static_fire_date_unix"), launch.get("net"), launch.get("window"),
                    launch.get("rocket"), launch.get("success"), launch.get("details"),
                    launch.get("launchpad"), launch.get("auto_update"), launch.get("tbd"),
                    launch.get("launch_library_id"), launch.get("upcoming")
                ))

                fairings = launch.get("fairings")
                if fairings: cur.execute("INSERT INTO spacex_data.raw_spacex_fairings_data (launch_id, reused, recovery_attempt, recovered) VALUES (%s, %s, %s, %s);", (launch_id, fairings.get("reused"), fairings.get("recovery_attempt"), fairings.get("recovered")))
                links = launch.get("links", {})
                patch = links.get("patch", {})
                reddit = links.get("reddit", {})
                cur.execute("INSERT INTO spacex_data.raw_spacex_links_data (launch_id, patch_small, patch_large, webcast, youtube_id, article, wikipedia, reddit_campaign, reddit_launch, reddit_media, reddit_recovery) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (launch_id, patch.get("small"), patch.get("large"), links.get("webcast"), links.get("youtube_id"), links.get("article"), links.get("wikipedia"), reddit.get("campaign"), reddit.get("launch"), reddit.get("media"), reddit.get("recovery")))
                for fail in launch.get("failures", []): cur.execute("INSERT INTO spacex_data.raw_spacex_failures_data (launch_id, time, altitude, reason) VALUES (%s, %s, %s, %s);", (launch_id, fail.get("time"), fail.get("altitude"), fail.get("reason")))
                for core in launch.get("cores", []): cur.execute("INSERT INTO spacex_data.raw_spacex_cores_data (launch_id, core, flight, gridfins, legs, reused, landing_attempt, landing_success, landing_type, landpad) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (launch_id, core.get("core"), core.get("flight"), core.get("gridfins"), core.get("legs"), core.get("reused"), core.get("landing_attempt"), core.get("landing_success"), core.get("landing_type"), core.get("landpad")))
                for payload_id in launch.get("payloads", []): cur.execute("INSERT INTO spacex_data.raw_spacex_launch_payloads_data (launch_id, payload_id) VALUES (%s, %s);", (launch_id, payload_id))
                for crew_id in launch.get("crew", []): cur.execute("INSERT INTO spacex_data.raw_spacex_launch_crew_data (launch_id, crew_id) VALUES (%s, %s);", (launch_id, crew_id))
                for ship_id in launch.get("ships", []): cur.execute("INSERT INTO spacex_data.raw_spacex_launch_ships_data (launch_id, ship_id) VALUES (%s, %s);", (launch_id, ship_id))
                for capsule_id in launch.get("capsules", []): cur.execute("INSERT INTO spacex_data.raw_spacex_launch_capsules_data (launch_id, capsule_id) VALUES (%s, %s);", (launch_id, capsule_id))

            except Exception as e:
                logger.error(f"Ошибка при обработке launch ID {launch_id}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
    logger.info("Данные о запусках успешно загружены.")

def main():
    conn = get_connection()
    if not conn:
        logger.error("Не удалось установить соединение с базой данных. Выход.")
        return

    logger.info("Соединение с базой данных установлено.")

    extract_create_table(conn)

    launches, rockets, payloads = fetch_data()

    if rockets:
        insert_rockets(conn, rockets)
    if payloads:
        insert_payloads(conn, payloads)
    if launches:
        insert_launches(conn, launches)

    if not any([launches, rockets, payloads]):
        logger.warning("Данные не были получены из API. Вставка данных не будет выполнена.")

    conn.close()
    logger.info("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()
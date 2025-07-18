import requests
import psycopg2
from datetime imgit init
import datetime, timedelta, time
import time as t
from config import DB_CONFIG


# Get date range: from 2022-01-01 to the day before yesterday
start_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
end_date = datetime.now() - timedelta(days=2)

print("Starting daily data fetch and insertion...")

# Connect to the database
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS purchases (
        client_id INTEGER,
        gender CHAR(1),
        purchase_datetime DATE,
        purchase_time_as_seconds_from_midnight INTEGER,
        purchase_time TIME,
        product_id INTEGER,
        quantity INTEGER,
        price_per_item INTEGER,
        discount_per_item INTEGER,
        total_price INTEGER
    )
    """)
    conn.commit()

    insert_query = """
    INSERT INTO purchases (
        client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight,
        purchase_time, product_id, quantity, price_per_item, discount_per_item, total_price
    ) VALUES (
        %(client_id)s, %(gender)s, %(purchase_datetime)s, %(purchase_time_as_seconds_from_midnight)s,
        %(purchase_time)s, %(product_id)s, %(quantity)s, %(price_per_item)s, %(discount_per_item)s, %(total_price)s
    )
    """

    # Iterate over each day in the date range
    while start_date <= end_date:
        date_str = start_date.strftime('%Y-%m-%d')
        url = "http://final-project.simulative.ru/data"
        params = {"date": date_str}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            day_data = response.json()

            clean_day_data = []

            for record in day_data:
                seconds = record.get('purchase_time_as_seconds_from_midnight')
                if not isinstance(seconds, int) or not (0 <= seconds < 86400):
                    continue
                purchase_time = time(
                    hour=seconds // 3600,
                    minute=(seconds % 3600) // 60,
                    second=seconds % 60
                )
                record['purchase_time'] = purchase_time.strftime("%H:%M:%S")
                clean_day_data.append(record)

            if clean_day_data:
                cursor.executemany(insert_query, clean_day_data)
                conn.commit()
                print(f"{date_str}: inserted {len(clean_day_data)} records")
            else:
                print(f"{date_str}: no valid data")

        except Exception as e:
            print(f"{date_str}: request error — {e}")

        start_date += timedelta(days=1)
        t.sleep(0.2)

except Exception as e:
    print("Database connection or query execution error:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
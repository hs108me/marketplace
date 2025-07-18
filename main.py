import requests
import psycopg2
from datetime import datetime, timedelta, time
import sys
from config import DB_CONFIG

# Get yesterday's date
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# Fetch data from API
url = "http://final-project.simulative.ru/data"
params = {"date": yesterday}

try:
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Records fetched: {len(data)}")
except Exception as e:
    print("Error while fetching data:", e)
    sys.exit(1)

# Validate time and add 'purchase_time' field
clean_data = []

for record in data:
    seconds = record.get('purchase_time_as_seconds_from_midnight')

    if not isinstance(seconds, int) or not (0 <= seconds < 86400):
        continue  # Skip invalid record

    # Add 'purchase_time' field
    purchase_time = time(hour=seconds // 3600, minute=(seconds % 3600) // 60, second=seconds % 60)
    record['purchase_time'] = purchase_time.strftime("%H:%M:%S")
    clean_data.append(record)

print(f"Cleaned and prepared records: {len(clean_data)}")

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create table if it doesn't exist
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

    # Insert data
    insert_query = """
    INSERT INTO purchases (
        client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight,
        purchase_time, product_id, quantity, price_per_item, discount_per_item, total_price
    ) VALUES (
        %(client_id)s, %(gender)s, %(purchase_datetime)s, %(purchase_time_as_seconds_from_midnight)s,
        %(purchase_time)s, %(product_id)s, %(quantity)s, %(price_per_item)s, %(discount_per_item)s, %(total_price)s
    )
    """

    cursor.executemany(insert_query, clean_data)
    conn.commit()
    print("\nData successfully inserted into the database.")

except Exception as e:
    print("Database error:", e)

finally:
    if 'cursor' in locals(): cursor.close()
    if 'conn' in locals(): conn.close()

# Marketplace Data Collection and Dashboard Project

This project collects and analyzes data from an online marketplace. It includes a PostgreSQL database, scheduled data collection, and a Metabase dashboard for insights.

## Project Structure

marketplace/
├── input_db.py # Collects all historical data and loads into PostgreSQL
├── main.py # Collects yesterday’s data, runs daily at 07:00 (cron)
├── config.py # Contains PostgreSQL connection settings
├── requirements.txt # Python dependencies
└── venv/ # Virtual environment (local use only)

## Project Description

- **Database**: PostgreSQL is installed and configured on the server.
- **Scripts**:
  - `input_db.py`: Fetches full historical data via marketplace IP, transforms and validates JSON, inserts into the database.
  - `main.py`: Fetches yesterday’s data and inserts it into the database. Scheduled to run daily at 07:00 via cron.
- **Configuration**: Connection settings (host, user, password, etc.) are stored in `config.py`.
- **Dashboard**: A Metabase dashboard called  
  **"Sales, Customers & Products Overview Dashboard"** is deployed on the server.

---

## How to Run

1. Clone the repository
2. Install dependencies:
pip install -r requirements.txt
3. Edit database connection settings in config.py
4. Run full data load (once):
input_db.py
5. Run daily update (already automated via cron):
main.py

## Metabase Sales, Customers & Products Overview Dashboard

URL: http://128.140.35.130/public/dashboard/ce2512af-9520-4152-bed4-8eb16c77f76c

## Cron Job
The script main.py is scheduled to run daily at 07:00 via cron on the server. No manual action is required.



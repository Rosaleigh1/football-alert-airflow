"""
DAG: football_daily_email_alert_v1
Description: Fetches Premier League fixtures for the current day and sends an email summary.
Schedule: Daily at 08:00 AM.
"""

from datetime import datetime, timedelta
import json
import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.email import send_email
#from airflow.models import Variable
import os

# Default DAG arguments
default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='football_daily_email_alert_v1',
    default_args=default_args,
    description='Fetch PL fixtures and send daily email alert',
    schedule_interval='0 8 * * *',  # Runs at 08:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['football', 'email', 'etl']
)
def football_pipeline():

    # Task 1: Fetch data from external API
    @task()
    def fetch_fixtures():
        """Extracts scheduled matches from Football Data API."""
        http = HttpHook(method='GET', http_conn_id='football_api_conn')
        
        # Endpoint for Premier League (PL) scheduled matches
        endpoint = '/v4/matches?status=SCHEDULED&competitions=PL'
        
        try:
            response = http.run(endpoint)
            if response.status_code == 200:
                data = json.loads(response.text)
                return data['matches']
            else:
                logging.error(f"API Error: {response.status_code}")
                return []
        except Exception as e:
            logging.error(f"Connection Failed: {e}")
            return []

    # Task 2: Save to Database
    @task()
    def save_to_postgres(matches):
        """Loads or updates match data into PostgreSQL."""
        if not matches:
            return "No matches to save."

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist (Idempotency)
        create_table_sql = """
            CREATE TABLE IF NOT EXISTS football_matches (
                match_id INT PRIMARY KEY,
                home_team VARCHAR(100),
                away_team VARCHAR(100),
                match_time TIMESTAMP,
                status VARCHAR(50),
                home_score INT DEFAULT 0,       
                away_score INT DEFAULT 0,       
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
            );
        """
        cursor.execute(create_table_sql)

        # Upsert Logic: Insert or Update if exists
        upsert_sql = """
            INSERT INTO football_matches (match_id, home_team, away_team, match_time, status, home_score, away_score)
            VALUES (%s, %s, %s, %s, %s, 0, 0)
            ON CONFLICT (match_id) DO UPDATE 
            SET status = EXCLUDED.status, 
                match_time = EXCLUDED.match_time,
                last_updated = CURRENT_TIMESTAMP;
        """

        count = 0
        today = datetime.today().date()

        for match in matches:
            match_date = datetime.strptime(match['utcDate'], "%Y-%m-%dT%H:%M:%SZ").date()
            
            # Filter only today's matches to save storage/processing
            if match_date == today:
                cursor.execute(upsert_sql, (
                    match['id'], 
                    match['homeTeam']['name'], 
                    match['awayTeam']['name'], 
                    match['utcDate'], 
                    match['status']
                ))
                count += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        return f"Saved {count} matches for today."

    # Task 3: Send Notification
    @task()
    def send_notification_email(matches):
        """Generates HTML report and sends email via SMTP."""
        today = datetime.today().date()
        today_matches = [
            m for m in matches 
            if datetime.strptime(m['utcDate'], "%Y-%m-%dT%H:%M:%SZ").date() == today
        ]

        if not today_matches:
            logging.info("No matches today, skipping email.")
            return

        # Prepare Email Subject & Content
        subject = f"⚽ Matchday Alert: {datetime.now().strftime('%d/%m/%Y')}"
        
        # Build HTML list
        list_items = ""
        for match in today_matches:
            # Convert UTC to Local Time (UTC+7)
            dt = datetime.strptime(match['utcDate'], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=7)
            time_str = dt.strftime("%H:%M")
            
            list_items += f"""
                <li style="padding: 10px; border-bottom: 1px solid #eee;">
                    🕒 <b>{time_str}</b> : 
                    <span style="color: #d93025;">{match['homeTeam']['name']}</span> vs 
                    <span style="color: #188038;">{match['awayTeam']['name']}</span>
                </li>
            """

        html_content = f"""
        <div style="font-family: Arial, sans-serif; color: #333;">
            <h2 style="color: #1a73e8;">📅 Premier League Fixtures Today</h2>
            <hr>
            <ul style="list-style-type: none; padding: 0;">
                {list_items}
            </ul>
            <br><p><i>Sent from Airflow Automation</i></p>
        </div>
        """

        # Retrieve email from Airflow Variables for security
        to_email = os.getenv("MY_EMAIL_ALERT")
        
        try:
            send_email(to=to_email, subject=subject, html_content=html_content)
            logging.info(f"Email sent successfully to {to_email}")
        except Exception as e:
            logging.error(f"Failed to send email: {e}")

    # Define DAG Flow
    matches_data = fetch_fixtures()
    save_status = save_to_postgres(matches_data)
    email_status = send_notification_email(matches_data)

    # Dependency: Fetch -> Save -> Email
    matches_data >> save_status
    matches_data >> email_status

# Instantiate the DAG
dag_instance = football_pipeline()
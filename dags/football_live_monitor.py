"""
DAG: football_live_monitor_v1
Description: Monitors live matches every 15 minutes. Updates DB and sends email alerts on goal events.
Schedule: Every 15 minutes.
"""

from datetime import datetime, timedelta
import json
import logging
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.email import send_email
#from airflow.models import Variable
import os
# Timezone Configuration (Asia/Bangkok)
local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='football_live_monitor_v1',
    default_args=default_args,
    description='Real-time match monitoring and goal alerts',
    schedule_interval='*/15 * * * *',  # Runs every 15 mins
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['football', 'live', 'alert']
)
def live_monitor_pipeline():

    @task()
    def check_and_alert():
        """
        Main Logic:
        1. Check DB for potential active matches (Optimization).
        2. If active, fetch live data from API.
        3. Compare API score vs DB score.
        4. Alert if goal detected & Update DB.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Step 1: Optimization - Check if any match is IN_PLAY or starting soon/recently started
        # This prevents unnecessary API calls when no games are on.
        sql_check = """
            SELECT COUNT(*) FROM football_matches 
            WHERE status = 'IN_PLAY' 
            OR (status = 'SCHEDULED' AND match_time BETWEEN NOW() - INTERVAL '15 minutes' AND NOW() + INTERVAL '3 hours')
        """
        cursor.execute(sql_check)
        count = cursor.fetchone()[0]
        
        if count == 0:
            logging.info("No active matches found. Skipping API call.")
            conn.close()
            return "No active matches"

        logging.info(f"Found {count} potential matches. Fetching live data...")

        # Step 2: Fetch Live Data from API
        http = HttpHook(method='GET', http_conn_id='football_api_conn')
        try:
            response = http.run('/v4/matches?status=IN_PLAY')
            if response.status_code != 200:
                raise Exception(f"API Error: {response.status_code}")
            
            live_data = json.loads(response.text)['matches']
        except Exception as e:
            logging.error(f"API Request Failed: {e}")
            conn.close()
            return 

        # Step 3: Process Matches
        alerts_sent = 0
        
        for match in live_data:
            match_id = match['id']
            
            # Retrieve previous state from DB
            cursor.execute("SELECT home_score, away_score FROM football_matches WHERE match_id = %s", (match_id,))
            result = cursor.fetchone()
            
            if result:
                db_home_score, db_away_score = result
                
                # Handle potential None values from API
                current_home = match['score']['fullTime']['home'] or 0
                current_away = match['score']['fullTime']['away'] or 0
                
                logging.info(f"Match {match_id}: DB({db_home_score}-{db_away_score}) vs API({current_home}-{current_away})")

                # Step 4: Detect Goal (Current Score > DB Score)
                if (current_home > db_home_score) or (current_away > db_away_score):
                    logging.info("🚨 GOAL DETECTED! Sending alert...")
                    
                    # Prepare Email
                    home_name = match['homeTeam']['name']
                    away_name = match['awayTeam']['name']
                    
                    subject = f"⚽ GOAL! {home_name} {current_home} - {current_away} {away_name}"
                    html_content = f"""
                        <h1 style="color:red;">⚽ GOAL!!!</h1>
                        <h2>{home_name} <span style="background-color:yellow; padding:0 5px;">{current_home} - {current_away}</span> {away_name}</h2>
                        <p>Status: {match['status']}</p>
                        <hr>
                        <p><i>Real-time alert from Airflow</i></p>
                    """
                    
                    to_email = os.getenv("MY_EMAIL_ALERT")
                    try:
                        send_email(to=to_email, subject=subject, html_content=html_content)
                        alerts_sent += 1
                    except Exception as e:
                        logging.error(f"Failed to send email: {e}")

                    # Step 5: Update DB with new score immediately
                    update_sql = """
                        UPDATE football_matches 
                        SET home_score = %s, away_score = %s, status = %s, last_updated = CURRENT_TIMESTAMP
                        WHERE match_id = %s
                    """
                    cursor.execute(update_sql, (current_home, current_away, match['status'], match_id))
                    conn.commit()
                
                # If status changed but no goal (e.g., Half-Time), just update status
                elif match['status'] != 'IN_PLAY': 
                     cursor.execute("UPDATE football_matches SET status = %s WHERE match_id = %s", (match['status'], match_id))
                     conn.commit()

        conn.close()
        return f"Process finished. Alerts sent: {alerts_sent}"

    check_and_alert()

# Instantiate the DAG
monitor_dag = live_monitor_pipeline()
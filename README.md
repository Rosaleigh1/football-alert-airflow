# ⚽ Premier League Football Alert System

![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.4-blue?logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)

An automated End-to-End Data Pipeline built with **Apache Airflow** and **Docker** to track Premier League matches. The system fetches match schedules, stores them in a **PostgreSQL** data warehouse, sends daily email briefings, and monitors live games to send real-time goal alerts via Email.

---

## 🚀 Key Features

### 1. 📅 Daily Matchday Briefing
- **DAG:** `football_daily_email_alert_v1`
- Runs every morning at **08:00 AM**.
- Fetches daily fixtures from [football-data.org](https://www.football-data.org/) API.
- Stores/Updates match details in **PostgreSQL** (Upsert logic).
- Sends a summary email listing all matches and times (converted to Local Time).

### 2. 🚨 Real-time Goal Monitor
- **DAG:** `football_live_monitor_v1`
- Runs every **15 minutes**.
- **Smart Optimization:** Checks the database first. If no matches are `IN_PLAY` or scheduled nearby, it **skips the API call** to save free-tier quota.
- Compares live API scores with the database.
- Sends an **instant email alert** when a goal is detected (Score Change).
- Updates the database automatically.

### 3. 🛡️ Security & Best Practices
- **No Hardcoded Credentials:** API Keys, Database passwords, and Email credentials are managed via **Environment Variables** and **Airflow Connections**.
- **Dockerized Environment:** easy to deploy and reproduce.

---

## 🛠️ Tech Stack

- **Orchestration:** Apache Airflow 2.10.4
- **Containerization:** Docker & Docker Compose
- **Database:** PostgreSQL 15
- **Language:** Python 3.12
- **External API:** football-data.org (Free Tier)
- **Notification:** SMTP (Gmail)

---

## 📂 Project Structure

```bash
├── dags/
│   ├── football_alert_email.py    # Daily ETL & Briefing DAG
│   ├── football_live_monitor.py   # Live Monitoring & Alerting DAG
├── logs/                          # Airflow logs (Git ignored)
├── plugins/
├── .env                           # Secrets (Git ignored)
├── .gitignore                     # Security rules
└── docker-compose.yaml            # Container orchestration

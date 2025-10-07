# Automated EOD Notification System

This system monitors an Oracle database for End of Day (EOD) operational statuses and sends targeted email alerts to ensure timely process completion. It is configured via a MongoDB database and environment variables.

## Features

- **Daily Monitoring**: A dedicated script (`daily_monitor.py`) checks for pending branch sign-outs, teller sign-outs, and un-authorized transactions.
- **Weekly Reporting**: A separate script (`weekly_report.py`) generates an aggregated summary of all delays from the past week.
- **Configurable**: Business rules, contacts, and settings are managed in MongoDB for easy updates without code changes.
- **Testable**: Includes a "dry run" mode (`EMAIL_MODE=LOG`) to test logic without sending real emails.

## Project Structure

- `daily_monitor.py`: The entry point for running the daily monitoring tasks.
- `weekly_report.py`: The entry point for generating the weekly summary report.
- `src/`: Contains the core application logic.
  - `data_manager.py`: The data abstraction layer. Handles connections to Oracle and MongoDB.
- `templates/`: Jinja2 HTML templates for emails.
- `assets/`: Contains image assets for email templates.

## Setup

1.  **Clone the repository.**
2.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure environment variables:**
    - Copy `.env.example` to a new file named `.env`.
    - Fill in the values for your Oracle, MongoDB, and SMTP credentials.

    ```ini
    # .env file
    # --- System Mode ---
    EMAIL_MODE=LOG             # LOG or SEND

    # --- Oracle DB Credentials ---
    ORACLE_USER=your_user
    # ... etc ...
    ```

## Execution

-   **Run Daily Monitoring:**
    ```bash
    python daily_monitor.py
    ```
-   **Run Weekly Summary Report:**
    ```bash
    python weekly_report.py
    ```
## Logging and Data Maintenance

The system features a robust logging system and an automated maintenance script for data lifecycle management.

- **File Logging**: All terminal output is saved to daily rotating log files located in the `logs/` directory within the project folder. This is configurable via `LOG_DIR` in the `.env` file.
- **Automated Archiving**: A new script, `log_maintenance.py`, is provided to be run every six months. This script will:
  1. Identify the 6-month period that ended 6 months ago (e.g., in December it targets Jan-Jun).
  2. Compress all relevant log files from that period into a single `.zip` archive.
  3. Securely archive data from the `eodDelayLogs` MongoDB collection for the same period using `mongodump`.
  4. Purge the archived data from the live MongoDB collection to keep it lean and performant.

## Prerequisites

- **Python 3** and all libraries in `requirements.txt`.
- Access to **Oracle** and **MongoDB** databases.
- The **`mongodump` command-line utility must be installed** and accessible in the system's PATH for the maintenance script to work. This tool is part of the MongoDB Database Tools package.

## Setup

1.  **Clone the repository.**
2.  **Create and activate a virtual environment.**
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure `.env` file:** Copy `.env.example` to `.env` and fill in your credentials. The `LOG_DIR` defaults to a `logs/` folder within the project.

## Execution

- **Run Daily Monitoring:** `python daily_monitor.py`
- **Run Weekly Summary Report:** `python weekly_report.py`
- **Run Bi-Annual Maintenance:** `python log_maintenance.py`

## Scheduling (Example Cron Jobs)

```crontab
# Daily check Mon-Sat
0 17 * * 1-6 /path/to/venv/bin/python /path/to/project/daily_monitor.py

# Weekly report Sunday at 5 PM
0 17 * * 0 /path/to/venv/bin/python /path/to/project/weekly_report.py

# Bi-annual maintenance on Jan 1st and Jul 1st at 2 AM
0 2 1 1,7 * /path/to/venv/bin/python /path/to/project/log_maintenance.py
import logging
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

from dateutil.relativedelta import relativedelta
from pymongo import MongoClient

from src.config import settings
from src.logger_setup import setup_logging

# --- PREREQUISITE: pip install python-dateutil ---

setup_logging()

def get_target_period_range():
    """Calculates the start and end datetime for the 6-month period that ended 6 months ago."""
    today = datetime.utcnow()
    if today.month <= 6:
        end_of_target_period = today.replace(year=today.year - 1, month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)
        start_of_target_period = today.replace(year=today.year - 1, month=7, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        end_of_target_period = today.replace(month=6, day=30, hour=23, minute=59, second=59, microsecond=999999)
        start_of_target_period = today.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    return start_of_target_period, end_of_target_period

def compress_log_files(start_date: datetime, end_date: datetime, log_dir: Path):
    """Finds, compresses, and deletes log files for the target 6-month period."""
    # --- MODIFICATION: New archive name format ---
    archive_name = f"{datetime.now().strftime('%Y%m%d')}_logs_archive_{start_date.strftime('%Y%m')}_to_{end_date.strftime('%Y%m')}.zip"
    archive_path = log_dir / archive_name

    log_files_to_compress = []
    current_month = start_date
    while current_month <= end_date:
        # --- MODIFICATION: New pattern to find date-prefixed log files ---
        log_pattern = f"{current_month.strftime('%Y%m')}*_eod_monitor.log"
        log_files_to_compress.extend(log_dir.glob(log_pattern))
        current_month += relativedelta(months=1)

    if not log_files_to_compress:
        logging.info(f"No log files found for the period {start_date.strftime('%B %Y')} to {end_date.strftime('%B %Y')}. Nothing to compress.")
        return

    logging.info(f"Found {len(log_files_to_compress)} log files for the target period.")
    
    try:
        with ZipFile(archive_path, 'w') as zipf:
            for file in log_files_to_compress:
                zipf.write(file, arcname=file.name)
        logging.info(f"Successfully created log archive: {archive_path}")

        for file in log_files_to_compress:
            file.unlink()
        logging.info("Deleted original log files after successful compression.")
    except Exception as e:
        logging.error(f"Failed to compress log files: {e}", exc_info=True)
        if archive_path.exists(): archive_path.unlink()
        logging.warning("Log compression failed. Original files have NOT been deleted.")

def archive_and_purge_mongodb(start: datetime, end: datetime, archive_dir: Path):
    """Exports and deletes documents from the eodDelayLogs collection for the target period."""
    if not shutil.which("mongodump"):
        logging.critical("'mongodump' command not found in system PATH. Cannot perform MongoDB archive.")
        return

    # --- MODIFICATION: New archive name format ---
    archive_file_name = f"{datetime.now().strftime('%Y%m%d')}_eodDelayLogs_archive_{start.strftime('%Y%m')}_to_{end.strftime('%Y%m')}.gz"
    archive_file = archive_dir / archive_file_name
    
    query = f"{{'timestamp': {{'$gte': ISODate('{start.isoformat()}Z'), '$lte': ISODate('{end.isoformat()}Z')}}}}"
    command = [
        "mongodump", "--uri", settings.MONGO_URI, "--collection", "eodDelayLogs",
        "--query", query, f"--archive={archive_file}", "--gzip"
    ]
    
    logging.info(f"Archiving MongoDB logs for period: {start.strftime('%B %Y')} to {end.strftime('%B %Y')}...")
    
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        logging.info(f"Successfully created MongoDB archive: {archive_file}")
        
        logging.info(f"Purging MongoDB logs for the same period from the live collection...")
        client = MongoClient(settings.MONGO_URI)
        db = client.get_default_database()
        
        delete_result = db.eodDelayLogs.delete_many({"timestamp": {"$gte": start, "$lte": end}})
        logging.info(f"Successfully deleted {delete_result.deleted_count} documents from MongoDB.")
        client.close()

    except subprocess.CalledProcessError as e:
        logging.error("mongodump command failed. MongoDB data has NOT been deleted.")
        logging.error(f"Stderr: {e.stderr}")
        if archive_file.exists(): archive_file.unlink()
    except Exception as e:
        logging.error(f"An error occurred during MongoDB archive/purge: {e}", exc_info=True)
        logging.warning("MongoDB data has NOT been deleted.")

def main():
    """Main entry point for the maintenance script."""
    logging.info("--- Starting Bi-Annual Log Maintenance Script ---")
    log_dir = Path(settings.LOG_DIR)
    start_date, end_date = get_target_period_range()
    logging.info(f"Targeting maintenance for period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    compress_log_files(start_date, end_date, log_dir)
    archive_and_purge_mongodb(start_date, end_date, log_dir)
    logging.info("--- Bi-Annual Log Maintenance Script Finished ---")

if __name__ == "__main__":
    main()
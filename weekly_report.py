import logging
import sys

from src.config import settings
from src.data_manager import DataManager
from src.email_service import EmailService
from src.logger_setup import setup_logging
from src.scenarios import run_weekly_report

setup_logging()

def main():
    """
    Entry point for the Weekly EOD Summary Report script.
    """
    logging.info("--- Starting Weekly EOD Summary Report Script ---")
    logging.info(f"Email mode: {settings.EMAIL_MODE}")

    try:
        data_manager = DataManager()
        email_service = EmailService()

        logging.info("Executing task: Weekly Summary Report")
        run_weekly_report(data_manager, email_service)

        logging.info("Weekly report script finished successfully.")

    except Exception as e:
        logging.critical(f"A critical error occurred during the weekly report run: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
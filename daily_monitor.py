import logging
import sys

from src.config import settings
from src.data_manager import DataManager
from src.email_service import EmailService
from src.logger_setup import setup_logging
from src.scenarios import (
    _monitor_branch_signouts,
    _monitor_branch_authorizations,
    # _monitor_head_office_authorizations,
    _monitor_teller_signouts,
    _monitor_common_authorizations,
    _send_all_consolidated_reports,
)

setup_logging()

def main():
    """
    Entry point for the Daily EOD Monitoring script.
    It runs each check, sends individual alerts to action-takers, and then sends
    a single consolidated report to all management and oversight groups.
    """
    logging.info("--- Starting Daily EOD Monitoring Script ---")
    logging.info(f"Email mode: {settings.EMAIL_MODE}")

    try:
        data_manager = DataManager()
        email_service = EmailService()
        
        # This dictionary will hold all the data found during the run.
        report_context = {}
        
        # --- Step 1: Run each check. They will send their own targeted alerts. ---
        # --- Each function will also return the data it found for the summary. ---
        
        logging.info("--- Running: Branch Sign-out Monitoring ---")
        report_context['branch_signouts'] = _monitor_branch_signouts(data_manager, email_service)
        
        logging.info("--- Running: Branch Financial Authorization Monitoring ---")
        report_context['branch_auths'] = _monitor_branch_authorizations(data_manager, email_service)
        
        # logging.info("--- Running: Head Office Financial Authorization Monitoring ---")
        # report_context['ho_auths'] = _monitor_head_office_authorizations(data_manager, email_service)
        
        logging.info("--- Running: Teller Sign-out Monitoring ---")
        report_context['teller_signouts'] = _monitor_teller_signouts(data_manager, email_service)
        
        logging.info("--- Running: Common Authorization Queue Monitoring ---")
        branch_common_auths, ho_common_auths = _monitor_common_authorizations(data_manager, email_service)
        report_context['branch_common_auths'] = branch_common_auths
        # report_context['ho_common_auths'] = ho_common_auths
        
        # --- Step 2: Send the final consolidated report to management. ---
        _send_all_consolidated_reports(data_manager, email_service, report_context)

        logging.info("Daily monitoring script finished successfully.")

    except Exception as e:
        logging.critical(f"A critical error occurred during the daily monitoring run: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
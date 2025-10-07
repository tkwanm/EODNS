import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """
    Centralized configuration class. Loads all settings from environment variables.
    """
    # --- System Mode ---
    EMAIL_MODE = os.getenv("EMAIL_MODE", "LOG")
    LOG_DIR = os.getenv("LOG_DIR") or "./logs"

    # --- Oracle DB Credentials ---
    ORACLE_USER = os.getenv("ORACLE_USER")
    ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
    ORACLE_DSN = os.getenv("ORACLE_DSN")

    # --- MongoDB Credentials ---
    MONGO_URI = os.getenv("MONGO_URI")

    # --- SMTP Email Settings ---
    SMTP_HOST = os.getenv("SMTP_HOST")
    SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
    SMTP_USER = os.getenv("SMTP_USER")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
    SENDER_EMAIL = os.getenv("SENDER_EMAIL")
    
    TEST_RECIPIENTS_STR = os.getenv("TEST_RECIPIENTS", "")
    TEST_RECIPIENTS = [email.strip() for email in TEST_RECIPIENTS_STR.split(',') if email.strip()]

    # --- MODIFICATION: Added all MongoDB systemSettings Keys ---
    IT_CORE_MONITORING_KEY = os.getenv("IT_CORE_MONITORING_KEY", "IT_CORE_MONITORING")
    BRANCH_DISTRIBUTION_CHANNELS_KEY = os.getenv("BRANCH_DISTRIBUTION_CHANNELS_KEY", "BRANCH_DISTRIBUTION_CHANNELS")
    SENIOR_MANAGEMENT_KEY = os.getenv("SENIOR_MANAGEMENT_KEY", "SENIOR_MANAGEMENT")
    FINANCE_SUPERVISORS_KEY = os.getenv("FINANCE_SUPERVISORS_KEY", "FINANCE_SUPERVISORS")
    CREDIT_SUPERVISORS_KEY = os.getenv("CREDIT_SUPERVISORS_KEY", "CREDIT_SUPERVISORS")

settings = Settings()
import logging
from datetime import datetime, timedelta

import oracledb
from pymongo import MongoClient

from .config import settings

class DataManager:
    """
    Data Abstraction Layer.
    Connects to Oracle for operational data and MongoDB for configuration.
    """
    def __init__(self):
        self._op_source = _OracleSource()
        self._config_source = _MongoSource()

    # --- Operational Data Methods ---
    def get_pending_signouts(self): return self._op_source.get_pending_signouts()
    def get_branch_authorizations(self): return self._op_source.get_branch_authorizations()
    def get_head_office_authorizations(self): return self._op_source.get_head_office_authorizations()
    def get_head_office_user_map(self): return self._op_source.get_head_office_user_map()
    def get_pending_common_authorizations(self): return self._op_source.get_pending_common_authorizations()
    def get_pending_teller_signouts(self): return self._op_source.get_pending_teller_signouts()

    # --- Configuration Data Methods ---
    def get_branch_config(self, branch_code: int): return self._config_source.get_branch_config(branch_code)
    def get_department_by_id(self, dept_id: str): return self._config_source.get_department_by_id(dept_id)
    def get_system_setting(self, setting_key: str): return self._config_source.get_system_setting(setting_key)
    def get_weekly_delay_stats(self): return self._config_source.get_weekly_delay_stats()
    def log_notification(self, log_entry: dict): self._config_source.log_notification(log_entry)


class _OracleSource:
    def __init__(self):
        try:
            self.pool = oracledb.create_pool(user=settings.ORACLE_USER, password=settings.ORACLE_PASSWORD, dsn=settings.ORACLE_DSN, min=1, max=2, increment=1)
            logging.info("Oracle connection pool created successfully.")
        except Exception as e:
            logging.critical(f"Failed to create Oracle connection pool: {e}")
            raise

    def _execute_query(self, query: str):
        with self.pool.acquire() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor]

    def get_pending_signouts(self, date_str: str = None):
        date_filter = f"TO_DATE('{date_str}', 'DD-MON-YYYY')" if date_str else "TRUNC(SYSDATE)"
        query = f"SELECT * FROM brnstatus WHERE BRNSTATUS_STATUS = 'I' AND BRNSTATUS_CURR_DATE = {date_filter}"
        logging.info(f"Querying for pending signouts with filter: {date_filter}")
        return self._execute_query(query)

    def get_branch_authorizations(self, date_str: str = None):
        date_filter = f"TO_DATE('{date_str}', 'DD-MON-YYYY')" if date_str else "TRUNC(SYSDATE)"
        query = f"SELECT * FROM bopauthq WHERE BOPAUTHQ_ENTRY_STATUS = 'N' AND BOPAUTHQ_TRAN_DATE_OF_TRAN = {date_filter} AND BOPAUTHQ_TRAN_BRN_CODE != 100"
        logging.info(f"Querying for BRANCH pending authorizations with filter: {date_filter}")
        return self._execute_query(query)

    def get_head_office_authorizations(self, date_str: str = None):
        date_filter = f"TO_DATE('{date_str}', 'DD-MON-YYYY')" if date_str else "TRUNC(SYSDATE)"
        query = f"SELECT * FROM bopauthq WHERE BOPAUTHQ_ENTRY_STATUS = 'N' AND BOPAUTHQ_TRAN_DATE_OF_TRAN = {date_filter} AND BOPAUTHQ_TRAN_BRN_CODE = 100"
        logging.info(f"Querying for HEAD OFFICE pending authorizations with filter: {date_filter}")
        return self._execute_query(query)

    def get_head_office_user_map(self):
        query = "SELECT USER_ID, USER_DEPT_CODE FROM users WHERE USER_BRANCH_CODE = 100"
        logging.info("Fetching Head Office user-to-department map from Oracle.")
        user_list = self._execute_query(query)
        return {user['USER_ID']: user['USER_DEPT_CODE'] for user in user_list}

    def get_pending_common_authorizations(self, date_str: str = None):
        date_filter = f"TO_DATE('{date_str}', 'DD-MON-YYYY')" if date_str else "TRUNC(SYSDATE)"
        query = f"SELECT * FROM TBAAUTHQ t WHERE TRUNC(TBAQ_ENTRY_DATE) = {date_filter}"
        logging.info("Querying for pending common authorizations.")
        return self._execute_query(query)

    def get_pending_teller_signouts(self, date_str: str = None):
        date_filter = f"TO_DATE('{date_str}', 'DD-MON-YYYY')" if date_str else "TRUNC(SYSDATE)"
        query = f"SELECT * FROM cashSIGNINOUT WHERE CASHSIGN_DATE = {date_filter} AND CASHSIGN_SIGNED_OUT = 0"
        logging.info(f"Querying for pending teller sign-outs with filter: {date_filter}")
        return self._execute_query(query)

class _MongoSource:
    def __init__(self):
        self.client = MongoClient(settings.MONGO_URI)
        self.db = self.client.get_default_database()
        logging.info("Connected to MongoDB.")

    def get_branch_config(self, branch_code: int):
        return self.db.branches.find_one({"_id": branch_code})
    
    def get_department_by_id(self, dept_id: str):
        return self.db.departments.find_one({"_id": dept_id})

    def get_system_setting(self, setting_key: str):
        setting = self.db.systemSettings.find_one({"_id": setting_key})
        return setting["value"] if setting else None

    def log_notification(self, log_entry: dict):
        self.db.eodDelayLogs.insert_one(log_entry)
        logging.info(f"Logged notification: {log_entry['delayType']} for {log_entry.get('branchId') or log_entry.get('departmentId')}")

    def get_weekly_delay_stats(self):
        today = datetime.utcnow(); start_of_this_week = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0); end_of_this_week = start_of_this_week + timedelta(days=6, hours=23, minutes=59, seconds=59); start_of_last_week = start_of_this_week - timedelta(weeks=1); end_of_last_week = start_of_last_week + timedelta(days=6, hours=23, minutes=59, seconds=59);
        def run_aggregation(start_date, end_date): pipeline = [{"$match": {"timestamp": {"$gte": start_date, "$lte": end_date}}}, {"$lookup": {"from": "branches", "localField": "branchId", "foreignField": "_id", "as": "branchInfo"}}, {"$lookup": {"from": "departments", "localField": "departmentId", "foreignField": "_id", "as": "deptInfo"}}, {"$addFields": {"groupName": {"$ifNull": [{"$arrayElemAt": ["$branchInfo.name", 0]}, {"$ifNull": [{"$arrayElemAt": ["$deptInfo.name", 0]}, "Unknown"]}]}}}, {"$facet": {"byGroup": [{"$group": {"_id": "$groupName", "totalDelays": {"$sum": 1}}}, {"$sort": {"totalDelays": -1}}], "byType": [{"$group": {"_id": "$delayType", "totalDelays": {"$sum": 1}}}]}}]; results = list(self.db.eodDelayLogs.aggregate(pipeline)); return results[0] if results else {"byGroup": [], "byType": []}
        stats_this_week = run_aggregation(start_of_this_week, end_of_this_week); stats_last_week = run_aggregation(start_of_last_week, end_of_last_week); total_incidents_this_week = sum(item['totalDelays'] for item in stats_this_week.get('byGroup', [])); total_incidents_last_week = sum(item['totalDelays'] for item in stats_last_week.get('byGroup', [])); trend_percent = "N/A"; trend_direction = "neutral";
        if total_incidents_last_week > 0: change = total_incidents_this_week - total_incidents_last_week; percent_change = (change / total_incidents_last_week) * 100; trend_percent = f"{percent_change:+.1f}%";
        top_offender_name = "N/A"; top_offender_count = 0;
        if stats_this_week.get('byGroup'): top_offender_name = stats_this_week['byGroup'][0]['_id']; top_offender_count = stats_this_week['byGroup'][0]['totalDelays']
        auth_delays = next((item['totalDelays'] for item in stats_this_week.get('byType', []) if item['_id'] == 'authorization'), 0); signout_delays = next((item['totalDelays'] for item in stats_this_week.get('byType', []) if item['_id'] == 'sign-out'), 0);
        return {"stats": stats_this_week, "startDate": start_of_this_week.strftime('%d-%b-%Y'), "endDate": end_of_this_week.strftime('%d-%b-%Y'), "metrics": {"total_incidents": total_incidents_this_week, "trend_percent": trend_percent, "trend_direction": trend_direction, "top_offender_name": top_offender_name, "top_offender_count": top_offender_count, "auth_delays": auth_delays, "signout_delays": signout_delays}}
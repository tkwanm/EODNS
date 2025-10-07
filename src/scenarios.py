import logging
from collections import defaultdict
from datetime import datetime

from .data_manager import DataManager
from .email_service import EmailService
from .config import settings

def _monitor_branch_signouts(data_manager: DataManager, email_service: EmailService):
    """Monitors and sends TARGETED alerts for branch signouts, then returns the pending data."""
    pending_branches = data_manager.get_pending_signouts()
    if not pending_branches:
        logging.info("Branch Signouts: No pending items found.")
        return []

    # Enrich data with branch names for the consolidated report
    for record in pending_branches:
        config = data_manager.get_branch_config(record["BRNSTATUS_BRN_CODE"])
        record['branch_name'] = config.get("name", "Unknown Branch") if config else "Unknown Branch"

    for branch_record in pending_branches:
        branch_code = branch_record["BRNSTATUS_BRN_CODE"]

        # --- MODIFICATION: Exclude Branch 100 from all sign-out notifications ---
        if branch_code == 100:
            logging.info(f"Branch 100 sign-out is pending but is excluded from notifications per business rules.")
            continue
        # --- END MODIFICATION ---

        branch_config = data_manager.get_branch_config(branch_code)
        if not branch_config: continue
        
        recipients = branch_config.get("supervisorEmails", [])
        if not recipients:
            logging.warning(f"No supervisors for branch {branch_code}. Cannot send targeted sign-out alert.")
            continue

        subject = f"Action Required: EOD Branch Sign-out Pending for {branch_record['branch_name']}"
        now = datetime.now()
        context = {"branch_name": branch_record['branch_name'], "current_date": now.strftime('%d-%b-%Y'), "timestamp": now.strftime('%Y-%m-%d %H:%M:%S')}
        email_service.send_email(recipients, subject, "branch_signout_alert.html", context)
        data_manager.log_notification({"timestamp": datetime.utcnow(), "delayType": "sign-out", "branchId": branch_code, "departmentId": None, "notificationSentTo": recipients})
    
    # Still return all data so it appears on the consolidated report, even if no alert was sent for Branch 100
    return pending_branches


def _monitor_branch_authorizations(data_manager: DataManager, email_service: EmailService):
    """Monitors and sends TARGETED alerts for branch financial auths, then returns the pending data."""
    pending_txns = data_manager.get_branch_authorizations()
    if not pending_txns:
        logging.info("Branch Financial Auths: No pending items found.")
        return []
    for txn in pending_txns:
        config = data_manager.get_branch_config(txn["BOPAUTHQ_TRAN_BRN_CODE"])
        txn['branch_name'] = config.get("name", "Unknown Branch") if config else "Unknown Branch"
    grouped_txns = defaultdict(list)
    for txn in pending_txns: grouped_txns[txn["BOPAUTHQ_TRAN_BRN_CODE"]].append(txn)
    for branch_code, transactions in grouped_txns.items():
        branch_config = data_manager.get_branch_config(branch_code)
        if not branch_config: continue
        recipients = branch_config.get("supervisorEmails", [])
        if not recipients: continue
        context = {"group_name": branch_config.get('name'), "transactions": transactions, "total_pending": len(transactions), "total_amount": f"{sum(t.get('BOPAUTHQ_AMT_INVOLVED_IN_BC') or 0 for t in transactions):,.2f}", "current_date": datetime.now().strftime('%d-%b-%Y'), "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        email_service.send_email(recipients, f"Urgent Action: Pending Transaction Authorizations for {branch_config.get('name')}", "transaction_auth_alert.html", context)
        data_manager.log_notification({"timestamp": datetime.utcnow(), "delayType": "authorization", "branchId": branch_code, "departmentId": None, "notificationSentTo": recipients})
    return pending_txns


# def _monitor_head_office_authorizations(data_manager: DataManager, email_service: EmailService):
#     """Monitors and sends TARGETED alerts for HO financial auths, then returns the ENRICHED pending data."""
#     pending_txns = data_manager.get_head_office_authorizations()
#     if not pending_txns:
#         logging.info("Head Office Financial Auths: No pending items found.")
#         return []
#     user_to_dept_code_map = data_manager.get_head_office_user_map()
#     dept_code_to_id_map = {"12": "CREDIT", "5": "FINANCE", "01": "RISK"}
#     grouped_txns = defaultdict(list)
#     for txn in pending_txns:
#         user_id = txn["BOPAUTHQ_ENTD_BY"]
#         dept_code = user_to_dept_code_map.get(user_id)
#         department_id = dept_code_to_id_map.get(str(dept_code)) if dept_code else None
#         if department_id:
#             config = data_manager.get_department_by_id(department_id)
#             txn['department_name'] = config.get("name", "Unknown Dept") if config else "Unknown Dept"
#             grouped_txns[department_id].append(txn)
#     for department_id, transactions in grouped_txns.items():
#         dept_config = data_manager.get_department_by_id(department_id)
#         if not dept_config: continue
#         recipients = dept_config.get("supervisorEmails", []) + dept_config.get("managerEmails", [])
#         if not recipients: continue
#         context = {"group_name": dept_config.get("name"), "transactions": transactions, "total_pending": len(transactions), "total_amount": f"{sum(t.get('BOPAUTHQ_AMT_INVOLVED_IN_BC') or 0 for t in transactions):,.2f}", "current_date": datetime.now().strftime('%d-%b-%Y'), "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#         email_service.send_email(recipients, f"Urgent Action: Pending Head Office Authorizations for {dept_config.get('name')}", "transaction_auth_alert.html", context)
#         data_manager.log_notification({"timestamp": datetime.utcnow(), "delayType": "authorization", "branchId": 100, "departmentId": department_id, "notificationSentTo": recipients})
#     enriched_transactions = [txn for txns_list in grouped_txns.values() for txn in txns_list]
#     return enriched_transactions


def _monitor_teller_signouts(data_manager: DataManager, email_service: EmailService):
    """Monitors and sends TARGETED alerts for teller signouts, then returns the pending data."""
    pending_tellers = data_manager.get_pending_teller_signouts()
    if not pending_tellers:
        logging.info("Teller Signouts: No pending items found.")
        return []
    for teller in pending_tellers:
        config = data_manager.get_branch_config(teller['CASHSIGN_BRN_CODE'])
        teller['branch_name'] = config.get("name", "Unknown Branch") if config else "Unknown Branch"
    grouped_by_branch = defaultdict(list)
    for teller in pending_tellers: 
        grouped_by_branch[teller['CASHSIGN_BRN_CODE']].append(teller['CASHSIGN_USER_ID'])
    for branch_code, teller_ids in grouped_by_branch.items():
        branch_config = data_manager.get_branch_config(branch_code)
        if not branch_config: continue
        recipients = branch_config.get("supervisorEmails", [])
        if not recipients: continue
        context = {"branch_name": branch_config.get("name"), "teller_ids": teller_ids, "current_date": datetime.now().strftime('%d-%b-%Y'), "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        email_service.send_email(recipients, f"Action Required: Pending Teller Sign-outs at {branch_config.get('name')}", "teller_signout_alert.html", context)
        data_manager.log_notification({"timestamp": datetime.utcnow(), "delayType": "teller-sign-out", "branchId": branch_code, "departmentId": None, "notificationSentTo": recipients})
    return pending_tellers


def _monitor_common_authorizations(data_manager: DataManager, email_service: EmailService):
    """Monitors and sends TARGETED alerts for common auths, then returns all pending data."""
    pending_items = data_manager.get_pending_common_authorizations()
    if not pending_items:
        logging.info("Common Auths: No pending items found.")
        return ({}, {})
    # user_to_dept_code_map = data_manager.get_head_office_user_map()
    # dept_code_to_id_map = {"12": "CREDIT", "5": "FINANCE"}
    branch_groups, ho_groups = defaultdict(list), defaultdict(list)
    for item in pending_items:
        branch_code = item.get('TBAQ_DONE_BRN')
        # if branch_code == 100:
        #     user_id = item.get('TBAQ_DONE_BY')
        #     dept_code = user_to_dept_code_map.get(user_id)
        #     department_id = dept_code_to_id_map.get(str(dept_code)) if dept_code else None
        #     if department_id: ho_groups[department_id].append(item)
        #elif
        if branch_code:
            branch_groups[branch_code].append(item)
    for branch_code, items in branch_groups.items():
        branch_config = data_manager.get_branch_config(branch_code)
        if not branch_config: continue
        recipients = branch_config.get("supervisorEmails", [])
        if not recipients: continue
        context = {"group_name": branch_config.get("name"), "items": items, "current_date": datetime.now().strftime('%d-%b-%Y'), "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        email_service.send_email(recipients, f"Action Required: Pending Common Authorizations for {branch_config.get('name')}", "common_auth_alert.html", context)
        data_manager.log_notification({"timestamp": datetime.utcnow(), "delayType": "common-auth", "branchId": branch_code, "departmentId": None, "notificationSentTo": recipients})
    for department_id, items in ho_groups.items():
        dept_config = data_manager.get_department_by_id(department_id)
        if not dept_config: continue
        recipients = dept_config.get("supervisorEmails", []) + dept_config.get("managerEmails", [])
        if not recipients: continue
        context = {"group_name": dept_config.get("name"), "items": items, "current_date": datetime.now().strftime('%d-%b-%Y'), "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        email_service.send_email(recipients, f"Action Required: Pending Common Authorizations for {dept_config.get('name')}", "common_auth_alert.html", context)
        data_manager.log_notification({"timestamp": datetime.utcnow(), "delayType": "common-auth", "branchId": 100, "departmentId": department_id, "notificationSentTo": recipients})
    return (branch_groups, ho_groups)


def _send_all_consolidated_reports(data_manager: DataManager, email_service: EmailService, context: dict):
    """Master function to generate and send the three distinct consolidated reports."""
    
    def generate_and_send_report(report_title, incidents_data, recipients, metrics_override=None):
        if not incidents_data:
            logging.info(f"No data for '{report_title}', skipping report.")
            return
        if not recipients:
            logging.warning(f"No recipients configured for '{report_title}'. Skipping.")
            return
        incidents_data.sort(key=lambda x: (x['group_name'], x['type']))
        grouped_data = defaultdict(list)
        for incident in incidents_data:
            grouped_data[(incident['branch_code'], incident['group_name'])].append(incident)
        now = datetime.now()
        report_context = {
            'report_title': report_title,
            'grouped_data': grouped_data,
            'metrics': metrics_override or {},
            'current_date': now.strftime('%d-%b-%Y'),
            'timestamp': now.strftime('%Y-%m-%d %H:%M:%S')
        }
        subject = f"{report_title} - {now.strftime('%d-%b-%Y')}"
        email_service.send_email(recipients, subject, "consolidated_report.html", report_context)

    it_monitoring = data_manager.get_system_setting(settings.IT_CORE_MONITORING_KEY) or []
    branch_distro = data_manager.get_system_setting(settings.BRANCH_DISTRIBUTION_CHANNELS_KEY) or []
    credit_sups = data_manager.get_system_setting(settings.CREDIT_SUPERVISORS_KEY) or []
    finance_sups = data_manager.get_system_setting(settings.FINANCE_SUPERVISORS_KEY) or []

    # --- Prepare Branch Report ---
    branch_incidents = []
    for item in context.get('branch_signouts', []):
        if item['BRNSTATUS_BRN_CODE'] != 100: branch_incidents.append({'group_name': item['branch_name'], 'branch_code': item['BRNSTATUS_BRN_CODE'], 'type': 'Branch Sign-out', 'details': f"Branch sign-out is pending."})
    for item in context.get('teller_signouts', []): branch_incidents.append({'group_name': item['branch_name'], 'branch_code': item['CASHSIGN_BRN_CODE'], 'type': 'Teller Sign-out', 'details': f"Teller ID: {item['CASHSIGN_USER_ID']}"})
    for item in context.get('branch_auths', []): branch_incidents.append({'group_name': item['branch_name'], 'branch_code': item['BOPAUTHQ_TRAN_BRN_CODE'], 'type': 'Financial Auth', 'details': f"Ref: {item['BOPAUTHQ_SOURCE_KEY_VALUE']} by {item['BOPAUTHQ_ENTD_BY']}"})
    for branch_code, items in context.get('branch_common_auths', {}).items():
        config = data_manager.get_branch_config(branch_code); name = config.get("name", "Unknown Branch") if config else "Unknown Branch"
        for item in items: branch_incidents.append({'group_name': name, 'branch_code': branch_code, 'type': 'Common Auth', 'details': f"Ref: {item['TBAQ_MAIN_PK']} by {item['TBAQ_DONE_BY']}"})
    branch_metrics = {'total_branch_signouts': len([i for i in branch_incidents if i['type']=='Branch Sign-out']), 'total_teller_signouts': len(context.get('teller_signouts', [])), 'total_financial_value': f"{sum(t.get('BOPAUTHQ_AMT_INVOLVED_IN_BC') or 0 for t in context.get('branch_auths', [])):,.2f}", 'total_common_auths': sum(len(v) for v in context.get('branch_common_auths', {}).values())}
    branch_recipients = list(set(it_monitoring + branch_distro))
    generate_and_send_report("Branch Operations Report", branch_incidents, branch_recipients, branch_metrics)

    # --- Prepare Credit Report ---
    credit_incidents = []
    for item in context.get('ho_auths', []):
        if item.get('department_name', '').startswith('Credit'): credit_incidents.append({'group_name': item['department_name'], 'branch_code': 100, 'type': 'Financial Auth', 'details': f"Ref: {item['BOPAUTHQ_SOURCE_KEY_VALUE']} by {item['BOPAUTHQ_ENTD_BY']}"})
    for dept_id, items in context.get('ho_common_auths', {}).items():
        if dept_id == 'CREDIT':
            config = data_manager.get_department_by_id(dept_id); name = config.get("name", "Unknown") if config else "Unknown"
            for item in items: credit_incidents.append({'group_name': name, 'branch_code': 100, 'type': 'Common Auth', 'details': f"Ref: {item['TBAQ_MAIN_PK']} by {item['TBAQ_DONE_BY']}"})
    credit_metrics = {'total_branch_signouts': 0, 'total_teller_signouts': 0, 'total_financial_value': f"{sum(t.get('BOPAUTHQ_AMT_INVOLVED_IN_BC') or 0 for t in context.get('ho_auths', []) if t.get('department_name', '').startswith('Credit')):,.2f}", 'total_common_auths': len(context.get('ho_common_auths', {}).get('CREDIT', []))}
    credit_recipients = list(set(it_monitoring + credit_sups))
    generate_and_send_report("Credit Department Report", credit_incidents, credit_recipients, credit_metrics)

    # --- Prepare Finance Report ---
    finance_incidents = []
    for item in context.get('ho_auths', []):
        if item.get('department_name', '').startswith('Finance'): finance_incidents.append({'group_name': item['department_name'], 'branch_code': 100, 'type': 'Financial Auth', 'details': f"Ref: {item['BOPAUTHQ_SOURCE_KEY_VALUE']} by {item['BOPAUTHQ_ENTD_BY']}"})
    for dept_id, items in context.get('ho_common_auths', {}).items():
        if dept_id == 'FINANCE':
            config = data_manager.get_department_by_id(dept_id); name = config.get("name", "Unknown") if config else "Unknown"
            for item in items: finance_incidents.append({'group_name': name, 'branch_code': 100, 'type': 'Common Auth', 'details': f"Ref: {item['TBAQ_MAIN_PK']} by {item['TBAQ_DONE_BY']}"})
    finance_metrics = {'total_branch_signouts': 0, 'total_teller_signouts': 0, 'total_financial_value': f"{sum(t.get('BOPAUTHQ_AMT_INVOLVED_IN_BC') or 0 for t in context.get('ho_auths', []) if t.get('department_name', '').startswith('Finance')):,.2f}", 'total_common_auths': len(context.get('ho_common_auths', {}).get('FINANCE', []))}
    finance_recipients = list(set(it_monitoring + finance_sups))
    generate_and_send_report("Finance Department Report", finance_incidents, finance_recipients, finance_metrics)


def run_weekly_report(data_manager: DataManager, email_service: EmailService):
    """Scenario 3: Weekly Summary Report."""
    logging.info("Generating weekly EOD delay summary report.")
    report_data = data_manager.get_weekly_delay_stats()
    if not report_data or not report_data.get("metrics"): return
    it_monitoring = data_manager.get_system_setting(settings.IT_CORE_MONITORING_KEY) or []
    senior_management = data_manager.get_system_setting(settings.SENIOR_MANAGEMENT_KEY) or []
    branch_distro = data_manager.get_system_setting(settings.BRANCH_DISTRIBUTION_CHANNELS_KEY) or []
    final_recipients = list(set(senior_management + it_monitoring + branch_distro))
    if not final_recipients: return
    start_date, end_date = report_data["startDate"], report_data["endDate"]
    subject = f"Weekly EOD Operations Summary: {start_date} to {end_date}"
    report_data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    email_service.send_email(final_recipients, subject, "weekly_summary_report.html", report_data)
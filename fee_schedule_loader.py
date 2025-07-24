from typing import Any
from context import Context
from database_connection import DatabaseConnection
from provider_bundle import ProviderBundle
from utilities import get_service_code_type

# --- DB queries ---
def fetch_schedule_metadata(context: Context, schedule_name: str) -> dict[str, Any]:
    query = f"""
        SELECT SCHEDULETYPE, ZIPSOURCETYPE
        FROM SCHEDULES
        WHERE SCHEDULECODE = '{schedule_name}'
    """
    rows = context.networx_conn.execute_query_with_columns(query)
    return rows[0] if rows else {}

def fetch_default_fee_schedule(context: Context, schedule_name: str) -> list[dict[str, Any]]:
    query = f"""
        SELECT *
        FROM SCHEDULEVALUESWITHMODIFIERS
        WHERE TABLENAME = '{schedule_name}'
        AND GETDATE() BETWEEN EFFECTIVEDATE AND TERMINATIONDATE
    """
    return context.networx_conn.execute_query_with_columns(query)

# --- Preloading logic for locality-based schedules ---
def preload_all_locality_fee_schedules(context: Context) -> dict:
    """
    Loads all rows from STATELOCALITYSCHEDULEVALUES into memory once,
    grouped by (TABLENAME, CARRIERNUMBER, LOCALITYNUMBER).
    """
    query = """
        SELECT *
        FROM STATELOCALITYSCHEDULEVALUES
        WHERE GETDATE() BETWEEN EFFECTIVEDATE AND TERMINATIONDATE
    """
    all_rows = context.networx_conn.execute_query_with_columns(query)

    locality_fee_schedules = {}

    for row in all_rows:
        schedule_name = row["TABLENAME"]
        carrier = row["CARRIERNUMBER"]
        locality = row["LOCALITYNUMBER"]
        key = (schedule_name, carrier, locality)

        parsed = process_fee_schedule_rows(context, schedule_name, [row])
        if key not in locality_fee_schedules:
            locality_fee_schedules[key] = {}

        for mod, procs in parsed.items():
            locality_fee_schedules[key].setdefault(mod, {}).update(procs)

    return locality_fee_schedules

def preload_fee_schedules(context: Context) -> dict:
    """
    Preloads all standard fee schedules used in rate sheets into context.fee_schedules.
    This avoids redundant loading during fee schedule processing.
    """
    query = """
    SELECT DISTINCT stdratesheetterms.actionparm1 AS schedulename
    FROM STDRATESHEETS
    LEFT JOIN STDRATESHEETTERMS
        ON STDRATESHEETS.RATESHEETID = STDRATESHEETTERMS.RATESHEETID
    INNER JOIN SCHEDULEVALUESWITHMODIFIERS
        ON STDRATESHEETTERMS.actionparm1 = SCHEDULEVALUESWITHMODIFIERS.tablename
    WHERE
        STDRATESHEETS.RATESHEETCODE IS NOT NULL AND
        STDRATESHEETS.RATESHEETCODE LIKE 'AV%' AND 
        STDRATESHEETS.RATESHEETCODE NOT LIKE 'AVGB%' AND 
        STDRATESHEETS.RATESHEETCODE NOT LIKE 'Z%' AND  
        STDRATESHEETS.RATESHEETCODE NOT LIKE '%-%'
    """
    context.fee_schedules = {}
    rows = context.networx_conn.execute_query_with_columns(query)

    for row in rows:
        schedule_name = row["schedulename"]
        if schedule_name not in context.fee_schedules:
            load_fee_schedule(context, schedule_name)
    

def process_fee_schedule_rows(
    context: Context, fee_schedule_name: str, rows: list[dict[str, Any]]
) -> dict:
    fee_schedule = {}
    for row in rows:
        proc_code = row.get("PROCEDURECODE", "")
        modifier = row.get("MODIFIER", "").strip()
        rate = float(row.get("ALLOWED", 0))
        percentage = float(row.get("PERCENTAGE", 0))
        term_date = row.get("TERMINATIONDATE", None)
        if term_date:
            term_date = term_date.strftime('%Y%m%d')

        proc_code_type = get_service_code_type(proc_code)

        temp_dict = {
            "modifier": modifier,
            "proc_code_type": proc_code_type,
            "allowed": rate,
            "percentage": percentage,
            "term_date": term_date
        }

        if modifier not in fee_schedule:
            fee_schedule[modifier] = {}

        fee_schedule[modifier][proc_code] = temp_dict

    return fee_schedule

def load_fee_schedule(context: Context, schedule_name: str) -> list[tuple[str, str, str]]:
    if schedule_name in context.fee_schedules:
        return []

    if not hasattr(context.shared_config, "locality_fee_schedules"):
        context.shared_config.locality_fee_schedules = {}

    if not hasattr(context.shared_config, "fee_schedule_types"):
        context.shared_config.fee_schedule_types = {}

    locality_keys = []

    metadata = fetch_schedule_metadata(context, schedule_name)
    schedule_type = metadata.get("SCHEDULETYPE", "")
    zip_source_type = metadata.get("ZIPSOURCETYPE", "")
    context.shared_config.fee_schedule_types[schedule_name] = schedule_type

    seen_keys = set()
    for carrier, locality, *_ in context.shared_config.locality_zip_ranges:
        key = (schedule_name, str(carrier), str(locality))
        if key in context.shared_config.locality_fee_schedules and key not in seen_keys:
            seen_keys.add(key)
            locality_keys.append(key)
            context.fee_schedules[schedule_name] = {}

    else:
        rows = fetch_default_fee_schedule(context, schedule_name)
        context.fee_schedules[schedule_name] = process_fee_schedule_rows(context, schedule_name, rows)

    return locality_keys

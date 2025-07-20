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

def fetch_all_locality_info(context: Context) -> None:
    if context.locality_zip_ranges is not None:
        return  # Already loaded

    query = """
        SELECT LOCALITYNUMBER, CARRIERNUMBER, BEGINZIP, ENDZIP
        FROM RBRVSZIP
        WHERE GETDATE() BETWEEN EFFECTIVEDATE AND TERMINATIONDATE
    """
    rows = context.networx_conn.execute_query_with_columns(query)

    context.locality_zip_ranges = [
        (
            row["CARRIERNUMBER"],
            row["LOCALITYNUMBER"],
            row["BEGINZIP"],
            row["ENDZIP"]
        )
        for row in rows
    ]

def fetch_locality_fee_schedule(
    context: Context, schedule_name: str, locality_number: str, carrier_number: str
) -> list[dict[str, Any]]:
    query = f"""
        SELECT *
        FROM STATELOCALITYSCHEDULEVALUES
        WHERE TABLENAME = '{schedule_name}'
        AND LOCALITYNUMBER = '{locality_number}'
        AND CARRIERNUMBER = '{carrier_number}'
        AND GETDATE() BETWEEN EFFECTIVEDATE AND TERMINATIONDATE
    """
    return context.networx_conn.execute_query_with_columns(query)

def fetch_default_fee_schedule(context: Context, schedule_name: str) -> list[dict[str, Any]]:
    query = f"""
        SELECT *
        FROM SCHEDULEVALUESWITHMODIFIERS
        WHERE TABLENAME = '{schedule_name}'
        AND GETDATE() BETWEEN EFFECTIVEDATE AND TERMINATIONDATE
    """
    return context.networx_conn.execute_query_with_columns(query)

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

# --- Main entry point ---
def load_fee_schedule(context: Context, schedule_name: str) -> None:
    # Standard schedules still use the top-level context.fee_schedules
    if schedule_name in context.fee_schedules:
        return

    metadata = fetch_schedule_metadata(context, schedule_name)
    schedule_type = metadata.get("SCHEDULETYPE", "")
    zip_source_type = metadata.get("ZIPSOURCETYPE", "")

    if schedule_type == "CarrierLocFeeSched":
        fetch_all_locality_info(context)

        # Make sure storage exists
        if not hasattr(context, "locality_fee_schedules"):
            context.locality_fee_schedules = {}

        for carrier, locality, *_ in context.locality_zip_ranges:
            key = (schedule_name, carrier, locality)

            if key in context.locality_fee_schedules:
                continue  # Already loaded

            sched_rows = fetch_locality_fee_schedule(context, schedule_name, locality, carrier)
            parsed_rows = process_fee_schedule_rows(context, schedule_name, sched_rows)

            context.locality_fee_schedules[key] = parsed_rows

        # Still mark this fee schedule as handled globally
        context.fee_schedules[schedule_name] = {}

    else:
        rows = fetch_default_fee_schedule(context, schedule_name)
        context.fee_schedules[schedule_name] = process_fee_schedule_rows(context, schedule_name, rows)
from ratesheet_loader import load_ratesheet_by_code  # You already have this
from typing import Any
from section_handlers import (
    process_inpatient_case_rate,
    process_inpatient_per_diem,
    process_inpatient_services,
    process_inpatient_exclusions,
    process_outpatient_case_rate,
    process_outpatient_per_diem,
    process_outpatient_services,
    process_outpatient_exclusions
)

def process_ratesheet_worker(
    batch: list[list[dict]],
    context: Any,
    shared_config: Any,
    networx_conn: Any,
    qnxt_conn: Any,
    tracker: Any,
    rate_file_writer: Any
) -> None:
    for ratesheet_group in batch:
        one_row = ratesheet_group[0]
        ratesheet_id = one_row["RATESHEETID"]
        rate_sheet_code = one_row.get("RATESHEETCODE", "")
        rate_cache: dict = {}

        #try:
            # Load sections + terms for this rate sheet
        ratesheet = load_ratesheet_by_code(context, rate_sheet_code)

        process_inpatient_per_diem(context, ratesheet.get("inpatient per diem", []), rate_cache)
        process_inpatient_case_rate(context, ratesheet.get("inpatient case rate", []), rate_cache)
        process_inpatient_services(context, ratesheet.get("inpatient services", []), rate_cache)
        process_inpatient_exclusions(context, ratesheet.get("inpatient exclusions", []), rate_cache)

        process_outpatient_services(context, ratesheet.get("outpatient services", []), rate_cache)
        process_outpatient_case_rate(context, ratesheet.get("outpatient case rate", []), rate_cache)
        process_outpatient_per_diem(context, ratesheet.get("outpatient per diem", []), rate_cache)
        process_outpatient_exclusions(context, ratesheet.get("outpatient exclusions", []), rate_cache)
        
        if not(len(rate_cache)):
            print()
            print(rate_sheet_code)
        else:
            print()
            print(f"{rate_sheet_code} -> OK")

        rate_file_writer.flush_cache(rate_cache)
        rate_cache.clear()

        # Mark this rate sheet as complete
        #tracker.mark_complete(ratesheet_id)

        #except Exception as e:
        #   print(f"❌ Error processing rate sheet {rate_sheet_code}: {e}")

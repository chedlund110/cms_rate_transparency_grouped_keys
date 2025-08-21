import os
from typing import Any, Iterator
from shared_config import SharedConfig
from context_factory import build_context
from database_connection import DatabaseConnection
from buffered_rate_file_writer import BufferedRateFileWriter
from ratesheet_batch_tracker import RateSheetBatchTracker
from ratesheet_logic import fetch_ratesheets, group_rows_by_ratesheet_id
from ratesheet_worker import process_ratesheet_worker
from rate_group_key_factory import RateGroupKeyFactory, merge_rate_group_key_factories

def process_ratesheets(shared_config: SharedConfig, networx_conn, qnxt_conn, mode:str = "full") -> RateGroupKeyFactory:
    # Set up tracking
    tracker_path = os.path.join(shared_config.directory_structure["status_tracker_dir"], "ratesheet_status.json")
    tracker: RateSheetBatchTracker = RateSheetBatchTracker(tracker_path)

    # Temporary context just to load rate sheet rows
    temp_context = build_context(shared_config, networx_conn, qnxt_conn)
    temp_context.rate_group_key_factory = RateGroupKeyFactory()
    temp_context.shared_config = shared_config
    temp_context.fee_schedules = shared_config.fee_schedules

    ratesheet_rows = fetch_ratesheets(temp_context)
    grouped_ratesheets = group_rows_by_ratesheet_id(ratesheet_rows)
    
    # Extract unique rate sheet codes
    all_codes = {
        row["RATESHEETCODE"]
        for rows in grouped_ratesheets.values()
        for row in rows
        if row.get("RATESHEETCODE")
    }
    #LIMIT = 10  # For testing
    #grouped_values = list(grouped_ratesheets.values())[:LIMIT]
    if mode == "full":
        tracker.initialize_from_list(sorted(all_codes))
        grouped_values = list(grouped_ratesheets.values())

    elif mode == "resume":
        pending_codes = set(tracker.get_pending_rate_sheets())
        grouped_values = [
            rows for rows in grouped_ratesheets.values()
            if rows[0]["RATESHEETCODE"] in pending_codes
        ]

    elif mode == "failed_only":
        failed_codes = {
            code for code, info in tracker.data.items()
            if info["status"] == "failed"
        }
        grouped_values = [
            rows for rows in grouped_ratesheets.values()
            if rows[0]["RATESHEETCODE"] in failed_codes
        ]

    else:
        raise ValueError(f"Unsupported run mode: {mode}")

    # LIMIT = 100  # Optional for testing
    # grouped_values = list(grouped_ratesheets.values())[:LIMIT]


    ratesheet_batches = chunk_ratesheet_groups(grouped_values, batch_size=5)

    rate_file_writer = BufferedRateFileWriter(
        target_directory=shared_config.directory_structure["temp_output_dir"] + "/negotiated",
        file_prefix="NEGOTIATED"
    )
    rate_group_factories = []
    optum_apc_ratesheet_ids = set()
    for batch in ratesheet_batches:
        factory, temp_optum_ratesheet_ids = process_ratesheet_worker(
            batch=batch,
            context=temp_context,
            shared_config=shared_config,
            networx_conn=networx_conn,
            qnxt_conn=qnxt_conn,
            tracker=tracker,
            rate_file_writer=rate_file_writer
        )
        rate_group_factories.append(factory)
        optum_apc_ratesheet_ids.update(temp_optum_ratesheet_ids)
    merged_keys = merge_rate_group_key_factories(rate_group_factories)
    rate_file_writer.close_all_files()
    return merged_keys, optum_apc_ratesheet_ids

def chunk_ratesheet_groups(grouped_ratesheet_values: list[list[dict]], batch_size: int) -> Iterator[list[list[dict]]]:
    for i in range(0, len(grouped_ratesheet_values), batch_size):
        yield grouped_ratesheet_values[i:i + batch_size]
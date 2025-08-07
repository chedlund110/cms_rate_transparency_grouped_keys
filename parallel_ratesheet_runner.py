import multiprocessing
import os
import uuid
from typing import Any, Iterator
from collections import defaultdict
from context_factory import build_context
from shared_config import SharedConfig
from database_connection import DatabaseConnection
from buffered_rate_file_writer import BufferedRateFileWriter
from pathlib import Path
from ratesheet_logic import fetch_ratesheets, group_rows_by_ratesheet_id
from ratesheet_batch_tracker import RateSheetBatchTracker
from rate_group_key_factory import RateGroupKeyFactory, merge_rate_group_key_factories

def parallel_process_ratesheets(shared_config: SharedConfig, mode: str = "full") -> RateGroupKeyFactory:
    tracker_path = os.path.join(shared_config.directory_structure["status_tracker_dir"], "ratesheet_status.json")

    os.makedirs(shared_config.directory_structure["temp_output_dir"], exist_ok=True)
    os.makedirs(os.path.join(shared_config.directory_structure["temp_output_dir"], "negotiated"), exist_ok=True)

    networx_conn = DatabaseConnection(shared_config.networx_connection_string)
    qnxt_conn = DatabaseConnection(shared_config.qnxt_connection_string)

    context = build_context(shared_config, networx_conn, qnxt_conn)
    context.rate_group_key_factory = RateGroupKeyFactory()
    context.shared_config = shared_config
    context.fee_schedules = shared_config.fee_schedules
    ratesheet_rows = fetch_ratesheets(context)
    grouped_ratesheets = group_rows_by_ratesheet_id(ratesheet_rows)

    # Extract unique rate sheet codes
    all_codes = {
        row["RATESHEETCODE"]
        for rows in grouped_ratesheets.values()
        for row in rows
        if row.get("RATESHEETCODE")
    }

    # Initialize tracker and apply mode filtering
    tracker = RateSheetBatchTracker(tracker_path)
    
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

    # Chunk for parallel processing
    batches = chunk_ratesheet_groups(grouped_values, batch_size=15)

    args_list = [
        (
            batch,
            shared_config,
            shared_config.networx_connection_string,
            shared_config.qnxt_connection_string,
            tracker_path
        )
        for batch in batches
    ]

    ctx = multiprocessing.get_context("spawn")
    num_processes = min(8, os.cpu_count() or 1)
    num_processes = 2
    with ctx.Pool(processes=num_processes) as pool:
        rate_group_key_factories = pool.starmap(process_ratesheet_batch_safe, args_list)

    merged_keys = merge_rate_group_key_factories(rate_group_key_factories)
    return merged_keys

def process_ratesheet_batch_safe(ratesheet_batch, shared_config, networx_conn_str, qnxt_conn_str, tracker_path):
    from ratesheet_worker import process_ratesheet_worker
    from database_connection import DatabaseConnection
    from ratesheet_batch_tracker import RateSheetBatchTracker
    from context_factory import build_context
    from file_writer import open_writer

    # Tracker for marking rate sheet status
    tracker = RateSheetBatchTracker(tracker_path)
    
    networx_conn = DatabaseConnection(networx_conn_str)
    qnxt_conn = DatabaseConnection(qnxt_conn_str)

    batch_uid = str(uuid.uuid4())[:8]
    batch_output_dir = os.path.join(shared_config.directory_structure["temp_output_dir"], f"batch_{batch_uid}")
    os.makedirs(batch_output_dir, exist_ok=True)

    provider_identifier_output_file = open_writer(os.path.join(batch_output_dir, "identifiers.txt"))
    prov_grp_contract_output_file = open_writer(os.path.join(batch_output_dir, "contractxref.txt"))

    negotiated_output_dir = os.path.join(shared_config.directory_structure["temp_output_dir"], "negotiated")
    os.makedirs(negotiated_output_dir, exist_ok=True)

    rate_file_writer = BufferedRateFileWriter(
        target_directory=negotiated_output_dir,
        file_prefix=f"NEGOTIATED_{batch_uid}"
    )

    context = build_context(shared_config, networx_conn, qnxt_conn)
    context.provider_identifier_output_file = provider_identifier_output_file
    context.prov_grp_contract_output_file = prov_grp_contract_output_file
    context.rate_file_writer = rate_file_writer

    try:
        rate_group_key_factory = process_ratesheet_worker(
            ratesheet_batch,
            context,
            shared_config,
            networx_conn,
            qnxt_conn,
            tracker,
            rate_file_writer
        )
        return rate_group_key_factory
    
    except Exception as e:
        print(f"❌ Error in batch {batch_uid}: {e}")
    finally:
        rate_file_writer.close_all_files()
        provider_identifier_output_file.close()
        prov_grp_contract_output_file.close()


def chunk_ratesheet_groups(grouped_ratesheet_values: list[list[dict]], batch_size: int) -> Iterator[list[list[dict]]]:
    for i in range(0, len(grouped_ratesheet_values), batch_size):
        yield grouped_ratesheet_values[i:i + batch_size]


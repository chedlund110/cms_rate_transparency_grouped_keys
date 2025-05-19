import multiprocessing
import os
import uuid
from typing import Any, Iterator
from context_factory import build_context
from shared_config import SharedConfig
from database_connection import DatabaseConnection
from file_batch_tracker import FileBatchTracker
from buffered_rate_file_writer import BufferedRateFileWriter
from ratesheet_logic import fetch_ratesheets, group_rows_by_ratesheet_id  # <-- You'll define this module

def parallel_process_ratesheets(shared_config: SharedConfig) -> None:
    tracker_path = os.path.join(shared_config.directory_structure["status_tracker_dir"], "ratesheet_status.json")
    tracker = FileBatchTracker(tracker_path)

    os.makedirs(shared_config.directory_structure["temp_output_dir"], exist_ok=True)
    os.makedirs(os.path.join(shared_config.directory_structure["temp_output_dir"], "negotiated"), exist_ok=True)

    networx_conn = DatabaseConnection(shared_config.networx_connection_string)
    qnxt_conn = DatabaseConnection(shared_config.qnxt_connection_string)

    context = build_context(shared_config, networx_conn, qnxt_conn)
    
    ratesheet_rows = fetch_ratesheets(context)
    grouped_ratesheets = group_rows_by_ratesheet_id(ratesheet_rows)

    LIMIT = 100  # Optional for testing
    grouped_values = list(grouped_ratesheets.values())[:LIMIT]

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

    with ctx.Pool(processes=num_processes) as pool:
        pool.starmap(process_ratesheet_batch_safe, args_list)


def process_ratesheet_batch_safe(ratesheet_batch, shared_config, networx_conn_str, qnxt_conn_str, tracker_path):
    from ratesheet_worker import process_ratesheet_worker
    from database_connection import DatabaseConnection
    from file_batch_tracker import FileBatchTracker
    from context_factory import build_context
    from file_writer import open_writer

    tracker = FileBatchTracker(tracker_path)
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
        process_ratesheet_worker(
            ratesheet_batch,
            context,
            shared_config,
            networx_conn,
            qnxt_conn,
            tracker,
            rate_file_writer
        )
    except Exception as e:
        print(f"❌ Error in batch {batch_uid}: {e}")
    finally:
        rate_file_writer.close_all_files()
        provider_identifier_output_file.close()
        prov_grp_contract_output_file.close()


def chunk_ratesheet_groups(grouped_ratesheet_values: list[list[dict]], batch_size: int) -> Iterator[list[list[dict]]]:
    for i in range(0, len(grouped_ratesheet_values), batch_size):
        yield grouped_ratesheet_values[i:i + batch_size]
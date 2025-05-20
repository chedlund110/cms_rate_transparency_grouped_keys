import os
from typing import Any, Iterator
from shared_config import SharedConfig
from context_factory import build_context
from database_connection import DatabaseConnection
from batch_tracker import BatchTracker
from file_batch_tracker import FileBatchTracker
from buffered_rate_file_writer import BufferedRateFileWriter
from ratesheet_logic import fetch_ratesheets, group_rows_by_ratesheet_id
from ratesheet_worker import process_ratesheet_worker


def process_ratesheets(shared_config: SharedConfig, networx_conn, qnxt_conn) -> None:
    # Set up tracking
    tracker_path = os.path.join(shared_config.directory_structure["status_tracker_dir"], "ratesheet_status.json")
    tracker: BatchTracker = FileBatchTracker(tracker_path)

    # Temporary context just to load rate sheet rows
    temp_context = build_context(shared_config, networx_conn, qnxt_conn)

    ratesheet_rows = fetch_ratesheets(temp_context)
    grouped_ratesheets = group_rows_by_ratesheet_id(ratesheet_rows)

    #LIMIT = 5  # For testing
    #grouped_values = list(grouped_ratesheets.values())[:LIMIT]
    grouped_values = list(grouped_ratesheets.values())

    ratesheet_batches = chunk_ratesheet_groups(grouped_values, batch_size=5)

    rate_file_writer = BufferedRateFileWriter(
        target_directory=shared_config.directory_structure["temp_output_dir"] + "/negotiated",
        file_prefix="NEGOTIATED"
    )

    for batch in ratesheet_batches:
        process_ratesheet_worker(
            batch=batch,
            context=temp_context,
            shared_config=shared_config,
            networx_conn=networx_conn,
            qnxt_conn=qnxt_conn,
            tracker=tracker,
            rate_file_writer=rate_file_writer
        )

    rate_file_writer.close_all_files()


def chunk_ratesheet_groups(grouped_ratesheet_values: list[list[dict]], batch_size: int) -> Iterator[list[list[dict]]]:
    for i in range(0, len(grouped_ratesheet_values), batch_size):
        yield grouped_ratesheet_values[i:i + batch_size]
from typing import Any, Iterator
from context import Context
from context_factory import build_context
from constants import TAXONOMY_ATTRIBUTE_ID
from codegroup_loader import load_code_groups
from provider_worker import process_provider_worker
from provider_logic import fetch_providers, group_provider_rows_by_unique_key
from utilities import build_in_clause_from_list
from batch_tracker import BatchTracker
from file_batch_tracker import FileBatchTracker
from shared_config import SharedConfig
from buffered_rate_file_writer import BufferedRateFileWriter
import os

def process_providers(shared_config: SharedConfig, networx_conn, qnxt_conn) -> None:
    # Fetch and group provider rows
    tracker_path = os.path.join(shared_config.directory_structure["status_tracker_dir"], "provider_status.json")
    tracker: BatchTracker = FileBatchTracker(tracker_path)   

    # If you need to fetch providers first (before processing), create a temp context
    temp_context = build_context(
        shared_config,
        networx_conn,
        qnxt_conn
    )
    
    provider_rows = fetch_providers(temp_context)
    grouped_providers = group_provider_rows_by_unique_key(provider_rows)
    
    LIMIT = 100  # For testing
    grouped_values = list(grouped_providers.values())[:LIMIT]
    
    # grouped_values = list(grouped_providers.values())

    # Chunk into batches of providers
    provider_batches = chunk_provider_groups(grouped_values, batch_size=5)

    rate_file_writer = BufferedRateFileWriter(
        target_directory="output/negotiated",
        file_prefix="NEGOTIATED"
    )
    for batch in provider_batches:
        process_provider_worker(
            provider_batch=batch,
            shared_config=shared_config,
            networx_conn=networx_conn,
            qnxt_conn=qnxt_conn,
            tracker=tracker,
            rate_file_writer=rate_file_writer
        )
    rate_file_writer.close_all_files()

def chunk_provider_groups(grouped_provider_values: list[list[dict]], batch_size: int) -> Iterator[list[list[dict]]]:
    for i in range(0, len(grouped_provider_values), batch_size):
        yield grouped_provider_values[i:i + batch_size]
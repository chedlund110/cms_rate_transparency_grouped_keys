
from shared_config import SharedConfig
from context_factory import build_context
from database_connection import DatabaseConnection
from file_batch_tracker import FileBatchTracker
from file_writer import open_writer
from provider_logic import process_single_provider, fetch_providers
from provider_bundle import build_provider_bundle
from rate_group_key_factory import RateGroupKeyFactory

def run_all_providers(shared_config: SharedConfig, rate_group_key_factory: RateGroupKeyFactory) -> None:
    tracker_path = shared_config.directory_structure["status_tracker_dir"] + "/provider_status.json"
    tracker = FileBatchTracker(tracker_path)

    networx_conn = DatabaseConnection(shared_config.networx_connection_string)
    qnxt_conn = DatabaseConnection(shared_config.qnxt_connection_string)

    context = build_context(shared_config, networx_conn, qnxt_conn)
    context.rate_group_key_factory = rate_group_key_factory

    provider_identifier_output_file = open_writer(
        shared_config.directory_structure["temp_output_dir"] + "/identifiers.txt"
    )
    prov_grp_contract_output_file = open_writer(
        shared_config.directory_structure["temp_output_dir"] + "/contractxref.txt"
    )

    context.provider_identifier_output_file = provider_identifier_output_file
    context.prov_grp_contract_output_file = prov_grp_contract_output_file

    provider_rows = fetch_providers(context)

    for row in provider_rows:
        bundle = build_provider_bundle(row)
        group_keys = context.rate_group_key_factory.store.get(bundle.rate_sheet_code, {})  # ✅ FIXED

        process_single_provider(bundle, group_keys, context)

    provider_identifier_output_file.close()
    prov_grp_contract_output_file.close()
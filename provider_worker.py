from buffered_rate_file_writer import BufferedRateFileWriter
from context import Context
from database_connection import DatabaseConnection
from file_writer import write_prov_grp_contract_file, write_rate_records
from provider_bundle import ProviderBundle
from provider_logic import build_provider_bundle_from_rows, group_provider_rows_by_unique_key, process_single_provider
from provider_worker_logger import setup_worker_logger
from shared_config import SharedConfig
from batch_tracker import BatchTracker
import os
from time import time
from pathlib import Path

def log_provider_progress(shared_config: SharedConfig, provider_bundle: ProviderBundle, elapsed_seconds: float) -> None:
    log_path = os.path.join(shared_config.directory_structure["log_dir"], "provider_progress.log")
    line = f"{provider_bundle.provid}-{provider_bundle.rate_sheet_code} |{len(provider_bundle.provider_rates_temp)}|{elapsed_seconds:.3f}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)

def process_provider_worker(
    provider_batch: list[list[dict]],
    context: Context,
    shared_config: SharedConfig,
    networx_conn: DatabaseConnection,
    qnxt_conn: DatabaseConnection,
    tracker: BatchTracker,
    rate_file_writer: BufferedRateFileWriter
):
    for provider_rows in provider_batch:
        if not provider_rows:
            continue

        start_time = time()

        provider_id = provider_rows[0].get("provid", "").strip()
        rate_sheet_code = provider_rows[0].get("NxRateSheetId", "").strip()

        # Set up logger per provider
        log_dir = shared_config.directory_structure["log_dir"]
        logger = setup_worker_logger(log_dir, provider_id, rate_sheet_code, True)
        logger.info("🔧 Starting provider processing...")

        try:
            # Attach the rate file writer and shared output files to context
            context.rate_file_writer = rate_file_writer

            provider_bundle = build_provider_bundle_from_rows(provider_rows)
            logger.info("📦 Provider bundle created.")

            provider_bundle = process_single_provider(context, provider_bundle)

            elapsed = time() - start_time
            log_provider_progress(shared_config, provider_bundle, elapsed)

            logger.info(f"📊 Fee schedule keys: {list(provider_bundle.provider_fee_schedules)}")
            logger.info(f"📊 Rates in temp store: {len(provider_bundle.provider_rates_temp)}")

            if provider_bundle.prov_grp_contract_keys:
                write_rate_records(context, provider_bundle)
                write_prov_grp_contract_file(context, provider_bundle)
                logger.info(f"🧮 Provider {provider_bundle.provid} wrote {len(provider_bundle.provider_rates_temp)} negotiated rates")

            context.provider_identifier_output_file.flush()
            context.prov_grp_contract_output_file.flush()

        except Exception as e:
            logger.error(f"❌ Error processing provider {provider_id} ({rate_sheet_code}): {e}", exc_info=True)

        finally:
            duration = round(time() - start_time, 2)
            logger.info(f"⏱️ Processing time: {duration} seconds.")


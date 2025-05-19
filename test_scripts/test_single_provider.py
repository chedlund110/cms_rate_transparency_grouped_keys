from context_factory import build_context
from provider_worker import process_provider_worker
from provider_runner import fetch_providers, group_provider_rows_by_unique_key
from shared_config import SharedConfig
import utilities
from database_connection import DatabaseConnection

def test_single_provider(shared_config: SharedConfig):

    # Load config like in main.py
    config = utilities.load_config("./config/config.json")

    # Set up connections
    networx_config = config["networx_database"]
    networx_conn = DatabaseConnection(
        f"Driver={networx_config['driver']};"
        f"Server={networx_config['server']};"
        f"Port={networx_config['port']};"
        f"Database={networx_config['database']};"
        f"Trusted_Connection={'yes' if networx_config['trusted_connection'] else 'no'};"
    )

    qnxt_config = config["qnxt_database"]
    qnxt_conn = DatabaseConnection(
        f"Driver={qnxt_config['driver']};"
        f"Server={qnxt_config['server']};"
        f"Port={qnxt_config['port']};"
        f"Database={qnxt_config['database']};"
        f"Trusted_Connection={'yes' if qnxt_config['trusted_connection'] else 'no'};"
    )

    # Create a lightweight context to fetch provider rows
    context = build_context(
        shared_config,
        networx_conn,
        qnxt_conn
    )

    provider_rows = fetch_providers(context)
    grouped = group_provider_rows_by_unique_key(provider_rows)

    if not grouped:
        print("⚠️ No providers found.")
        return

    # Pick the first provider group
    first_key = next(iter(grouped))
    first_provider_group = grouped[first_key]
    print(f"Running test for provider {first_key}...")

    process_provider_worker(first_provider_group, shared_config, networx_conn, qnxt_conn)

    print("✅ Test complete. Check your output directory.")

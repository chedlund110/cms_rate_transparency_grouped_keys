import os
import cProfile
import pstats
from datetime import datetime

from billing_code_extract import BillingCodeExtract
from place_of_service_extract import PlaceOfServiceExtract
from plan_detail_extract import PlanDetailExtract
from clean_output_folders import clear_output_folders
from codegroup_loader import load_code_groups
from context_factory import build_context
from database_connection import DatabaseConnection
from merge_output_files import merge_all_outputs
from parallel_runner import parallel_process_providers
from setup_environment import ensure_directories_exist
from shared_config import SharedConfig
import utilities


def load_config_and_shared_config():
    config = utilities.load_config("./config/config.json")
    if config is None:
        raise RuntimeError("Config could not be loaded.")

    base_dir = config["app_base_directory"]
    directory_structure = {
        key: os.path.join(base_dir, path)
        for key, path in config["directory_structure"].items()
    }

    shared_config = SharedConfig(
        reporting_entity=config["reporting_entity"],
        reporting_entity_type=config["reporting_entity_type"],
        insurer_code=config["insurer_code"],
        program_list=config["programs"],
        provider_code_range_types=config.get("provider_code_range_types", {}),
        service_code_range_types=config.get("service_code_range_types", {}),
        service_code_companion_range_types=config.get("service_code_companion_range_types", {}),
        case_rate_calcs=config["case_rate_calcs"],
        per_diem_calcs=config["per_diem_calcs"],
        fee_schedule_calcs=config["fee_schedule_calcs"],
        mrf_target_directory=os.path.join(base_dir, config["directory_structure"]["mrf_output_dir"]),
        mrf_file_prefixes=config["mrf_file_prefixes"],
        provider_identifier_full_path=None,
        prov_grp_contract_full_path=None,
        networx_connection_string=build_connection_string(config["networx_database"]),
        qnxt_connection_string=build_connection_string(config["qnxt_database"]),
        directory_structure=directory_structure
    )

    return shared_config


def build_connection_string(db_config):
    return (
        f"Driver={db_config['driver']};"
        f"Server={db_config['server']};"
        f"Port={db_config['port']};"
        f"Database={db_config['database']};"
        f"Trusted_Connection={'yes' if db_config['trusted_connection'] else 'no'};"
    )


def write_single_output_file(extract_class, filename_prefix, filename_ext, qnxt_conn, shared_config):
    filename = utilities.format_date_for_filename(
        shared_config.mrf_file_prefixes[filename_prefix],
        shared_config.mrf_file_prefixes[filename_ext]
    )
    full_path = os.path.join(shared_config.mrf_target_directory, filename)

    with open(full_path, mode='w', encoding='utf-8') as output_file:
        extract = extract_class({
            "db_conn": qnxt_conn,
            "output_file": output_file,
            "reporting_entity": shared_config.reporting_entity,
            "reporting_entity_type": shared_config.reporting_entity_type,
            "insurer_code": shared_config.insurer_code,
            "program_list": shared_config.program_list
        })
        extract.extract_data()
        utilities.create_mms_file(full_path, extract.records_processed)


def main():
    # Load code groups before processing providers
    shared_config = load_config_and_shared_config()

   # ensure_directories_exist(shared_config)
   # clear_output_folders(shared_config)

    networx_conn = DatabaseConnection(shared_config.networx_connection_string)
    qnxt_conn = DatabaseConnection(shared_config.qnxt_connection_string)

    context = build_context(shared_config, networx_conn, qnxt_conn)

    write_single_output_file(BillingCodeExtract, "billing_code", "billing_code_ext", qnxt_conn, shared_config)
    write_single_output_file(PlaceOfServiceExtract, "place_of_service", "place_of_service_ext", qnxt_conn, shared_config)
    write_single_output_file(PlanDetailExtract, "plan_detail", "plan_detail_ext", qnxt_conn, shared_config)

    # load code groups upfront / cache
    #load_code_groups(context)

    # this is the process that kicks off the rate subprocesses
    #parallel_process_providers(shared_config)

    # finally, merge all files
    #merge_all_outputs(shared_config)

if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()

    main()

    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats(50)
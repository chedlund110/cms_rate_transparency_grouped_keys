"""
main.py

Entry point for the process.

Chris Hedlund - 2025

"""

from billing_code_extract import BillingCodeExtract
from clean_output_folders import clear_output_folders 
from codegroup_loader import load_code_groups, load_ambsurg_codes, load_ndc_codes, load_drg_weights, load_locality_zip_ranges
from context import Context
from context_factory import build_context
import cProfile
from database_connection import DatabaseConnection
from datetime import datetime
from fee_schedule_loader import preload_all_locality_fee_schedules
import json
from merge_output_files import merge_all_outputs
from modifier_loader import load_modifier_map
import os
from ratesheet_runner import process_ratesheets
from parallel_ratesheet_runner import parallel_process_ratesheets
from time import sleep
from pathlib import Path
from place_of_service_extract import PlaceOfServiceExtract
from plan_detail_extract import PlanDetailExtract
from profiler import Profiler
from provider_runner import run_all_providers
import pstats
from rate_group_key_factory import RateGroupKeyFactory
from setup_environment import ensure_directories_exist
from shared_config import SharedConfig
import utilities

def process_billing_codes(context: Context, shared_config, base_params) -> None:
    valid_service_codes: set = set()
    billing_code_filename = context.shared_config.mrf_file_prefixes['billing_code']
    billing_code_ext = context.shared_config.mrf_file_prefixes['billing_code_ext']
    billing_code_filename = utilities.format_date_for_filename(billing_code_filename,billing_code_ext)
    billing_code_full_path = os.path.join(context.shared_config.mrf_target_directory,billing_code_filename)
    billing_output_file = open(billing_code_full_path, mode='w', encoding='utf-8')
    billing_code_extract = BillingCodeExtract(context.qnxt_conn, billing_output_file)
    valid_service_codes = billing_code_extract.extract_data()
    utilities.create_mms_file(billing_code_full_path,billing_code_extract.records_processed)
    shared_config.valid_service_codes = valid_service_codes

def process_place_of_service_codes(context: Context, base_params):
    place_of_service_filename = context.shared_config.mrf_file_prefixes["place_of_service"]
    place_of_service_ext = context.shared_config.mrf_file_prefixes['place_of_service_ext']
    place_of_service_filename = utilities.format_date_for_filename(place_of_service_filename,place_of_service_ext)
    place_of_service_full_path = os.path.join(context.shared_config.mrf_target_directory, place_of_service_filename)
    place_of_service_output_file = open(place_of_service_full_path, mode='w', encoding='utf-8')
    params = base_params.copy()
    params.update({
        "db_conn":context.qnxt_conn,
        "output_file":place_of_service_output_file
    })
    place_of_service_extract = PlaceOfServiceExtract(params)
    place_of_service_extract.extract_data()
    utilities.create_mms_file(place_of_service_full_path,place_of_service_extract.records_processed)

def process_plan_details(context: Context, base_params):
    plan_detail_filename = context.shared_config.mrf_file_prefixes["plan_detail"]
    plan_detail_ext = context.shared_config.mrf_file_prefixes['plan_detail_ext']
    plan_detail_filename = utilities.format_date_for_filename(plan_detail_filename,plan_detail_ext)
    plan_detail_full_path = os.path.join(context.shared_config.mrf_target_directory, plan_detail_filename)
    plan_detail_output_file = open(plan_detail_full_path, mode='w', encoding='utf-8')
    params = base_params.copy()
    params.update({
        "db_conn":context.qnxt_conn,
        "output_file":plan_detail_output_file
    })
    plan_detail_extract = PlanDetailExtract(params)
    plan_detail_extract.extract_data()
    utilities.create_mms_file(plan_detail_full_path,plan_detail_extract.records_processed)
        
def main():

    # The parameters to run the rpocess are in config.json
    # This includes base filenames, field constants, etc.

    config = utilities.load_config("./config/config.json")
    if config is None:
        return

    # Build common base directory
    app_base_dir = config["app_base_directory"]

    # Build the structured output folders
    base_dir = config["app_base_directory"]
    directory_structure = {
        key: os.path.join(base_dir, path)
        for key, path in config["directory_structure"].items()
    }
    program_list: list = config['programs']
    reporting_entity: str = config["reporting_entity"]
    reporting_entity_type: str = config["reporting_entity_type"]
    insurer_code: str = config["insurer_code"]

    # the calculation methods are stored in the config
    # that way we don't have to hardcode in the logic
    provider_code_range_types = config.get("provider_code_range_types",{})
    service_code_range_types: list = config.get("service_code_range_types",{})
    service_code_companion_range_types: list = config.get("service_code_companion_range_types",{})

    # base_params holds all the setup information
    # every extract class needs to function
    # class-specific parameters will be added as needed
    base_params = {
        "reporting_entity":reporting_entity,
        "reporting_entity_type":reporting_entity_type,
        "insurer_code":insurer_code,
        "program_list":program_list
    }

    # networx server connection - database networx
    networx_db_config = config['networx_database']
    networx_connection_string = (f"Driver={networx_db_config['driver']};"f"Server={networx_db_config['server']};"f"Port={networx_db_config['port']};"f"Database={networx_db_config['database']};"f"Trusted_Connection={'yes' if networx_db_config['trusted_connection'] else 'no'};")
    
    # QNXT server connection - database PlanData
    qnxt_db_config = config["qnxt_database"]
    qnxt_connection_string = (f"Driver={qnxt_db_config['driver']};"f"Server={qnxt_db_config['server']};"f"Port={qnxt_db_config['port']};"f"Database={qnxt_db_config['database']};"f"Trusted_Connection={'yes' if qnxt_db_config['trusted_connection'] else 'no'};")
    
    # use the DatabaseConnection object
    networx_conn = DatabaseConnection(networx_connection_string)
    qnxt_conn = DatabaseConnection(qnxt_connection_string)

    mrf_target_directory = directory_structure["mrf_output_dir"]
    mrf_file_prefixes = config['mrf_file_prefixes']

    shared_config = SharedConfig(
        reporting_entity=reporting_entity,
        reporting_entity_type=reporting_entity_type,
        insurer_code=insurer_code,
        program_list=program_list,
        provider_code_range_types=provider_code_range_types,
        service_code_range_types=service_code_range_types,
        service_code_companion_range_types=service_code_companion_range_types,
        mrf_target_directory=mrf_target_directory,
        mrf_file_prefixes=mrf_file_prefixes,
        provider_identifier_full_path=None,  # now handled per-provider
        prov_grp_contract_full_path=None,
        networx_connection_string=networx_connection_string,
        qnxt_connection_string=qnxt_connection_string,
        directory_structure=directory_structure
        )
    shared_config.codegroups = load_code_groups(networx_conn)
    shared_config.amb_surg_codes = load_ambsurg_codes(networx_conn)
    shared_config.ndc_codes = load_ndc_codes(networx_conn)
    shared_config.drg_weights = load_drg_weights(networx_conn)
    shared_config.locality_zip_ranges = load_locality_zip_ranges(networx_conn)

    reference_dir = shared_config.directory_structure["reference_dir"]
    modifier_path = os.path.join(reference_dir, "procedure_modifier_map.txt")
    shared_config.modifier_map = load_modifier_map(modifier_path)
    context = build_context(shared_config, networx_conn, qnxt_conn)
    shared_config.locality_fee_schedules = preload_all_locality_fee_schedules(context)
    
    ensure_directories_exist(shared_config)

    """
    ### NEED THIS PUT BACK IN ###

    # clear_output_folders(shared_config=shared_config)
    """

    # Stand-alone extracts
    process_billing_codes(context, shared_config, base_params)
    process_place_of_service_codes(context, base_params)
    process_plan_details(context, base_params)
    
    # standalone rate runner
    rate_group_key_factory: RateGroupKeyFactory = process_ratesheets(shared_config, networx_conn, qnxt_conn)
    
    # parallel process runner
    # rate_group_key_factory: RateGroupKeyFactory = parallel_process_ratesheets(shared_config)

    #run_all_providers(shared_config, rate_group_key_factory)

    #merge_all_outputs(shared_config)
    
if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    
    main()  
    
    profiler.disable()
    stats = pstats.Stats(profiler).sort_stats("cumtime")
    stats.print_stats(50)  # top 50 slowest functions

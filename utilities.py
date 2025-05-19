import json
from datetime import datetime
from typing import Any
from provider_bundle import ProviderBundle
import os
import re

def load_config(config_path) -> list[dict[str,Any]]:
    """Loads the JSON config file."""
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

def format_date_for_filename(filename, file_ext) -> str:
    return filename + datetime.now().strftime("%Y%m%d_%H%M%S") + "." + file_ext

import re

def build_nested_dir(base_dir: str, provider_id: str, rate_sheet_code: str) -> str:
    clean_id = provider_id.strip()
    if not clean_id:
        subpath = "_unknown"
    else:
        # Only use letters/numbers
        match = re.match(r"([A-Z]{3})(\d{2})", clean_id)
        if match:
            prefix, digits = match.groups()
            subpath = os.path.join(prefix, digits)
        else:
            # fallback to first 2 characters if no match
            subpath = os.path.join(clean_id[:2])

    final_dir = os.path.join(base_dir, subpath, f"{provider_id}_{rate_sheet_code}")
    os.makedirs(final_dir, exist_ok=True)
    return final_dir

def create_mms_file(filename, rec_cnt) -> None:
    mms_filename = filename.split('.')[0] + ".MMS"
    output_file = open(mms_filename, mode='w', encoding='utf-8')
    mms_line = "Number of Records:" + str(rec_cnt)
    output_file.write(mms_line)
    output_file.close()

def build_in_clause_from_list(list) -> str:
    output_string = ""
    output_string = ", ".join([f"'{list_item}'" for list_item in list])
    output_string = '(' + output_string + ')'
    return output_string


def get_dict_value(d: dict, key: str, default=None, strip=False, value_type=None):
    """
    Minimalist replacement: Fast lookup with optional strip.
    Type enforcement and error catching are skipped for performance.
    """
    value = d.get(key, default)
    if strip and isinstance(value, str):
        value = value.strip()
    return value
    """
    # extract a value from a dict
    # and handle all the defaults in one routine

    value = data.get(key, default)
    
    if not value:
        return default  # Return default if key is missing or value is None

    if strip and isinstance(value, str):
        value = value.strip()

    if value_type and not isinstance(value, value_type):
        try:
            value = value_type(value)  # Convert to expected type
        except (ValueError, TypeError):
            return default  # Return default if conversion fails
    return value
    """
def get_pos_and_type(section_name: str) -> tuple[str, str]:

    if section_name[0:9] == "inpatient":
        rate_type_desc: str = "institutional"
        rate_pos: str = "21"
    else:
        rate_type_desc: str = "professional"
        rate_pos: str = "11"
    return (rate_pos, rate_type_desc)

def get_fee_and_type(allow_amt: float, percentage: float) -> tuple:
    if percentage > 0:
        fee = percentage
        fee_type = "percentage"
    else:
        fee = allow_amt
        fee_type = "negotiated"
    return (fee, fee_type)

def get_service_code_type(proc_code: str) -> str:
    proc_code_type: str = ''
    if len(proc_code) == 5:
        if proc_code[0].isalpha():
            proc_code_type = "HCPCS"
        else:
            proc_code_type = "CPT"
    else:
        proc_code_type = "RC"
    return proc_code_type

def update_prov_grp_contract_keys(provider_bundle: ProviderBundle, rate_key: str = '') -> bool:
    xref_key = (provider_bundle.provid, provider_bundle.rate_sheet_code)
    xref_exists = xref_key in provider_bundle.prov_grp_contract_keys

    # Ensure the entry exists
    xref_entry = provider_bundle.prov_grp_contract_keys.setdefault(
        xref_key,
        {"programs": set(), "rate_keys": set()}
    )

    # Add all programs tied to this provider/rate_sheet
    for program_id in provider_bundle.program_list:
        xref_entry["programs"].add(program_id)

    # Add the rate_key if it’s valid
    if rate_key:
        xref_entry["rate_keys"].add(rate_key)

    return xref_exists  # True if key existed before, False if it was just created
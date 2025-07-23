from constants import DEFAULT_EXP_DATE
from rate_group_key_factory import RateGroupKey,RateGroupKeyFactory
from typing import Optional

def store_rate_record(
    rate_cache: dict,
    dict_key: tuple,
    rate_dict: dict,
    group_key: str = None,
    key_factory: RateGroupKeyFactory = None,
    code_tuple: tuple[str, str, str] = None,
    valid_service_codes: tuple[str,str] = None
) -> None:
    """
    Stores a rate record for a single rate sheet using a temporary dict.
    Also updates the group key with the procedure code used.
    """
    proc_code, modifier, pos = code_tuple
    billing_code_type = rate_dict["billing_code_type"]
    if billing_code_type == 'RC':
        temp_proc_code = proc_code.zfill(4)
    else:
        temp_proc_code = proc_code

    valid_code_key: str = (temp_proc_code, billing_code_type)
    if valid_code_key not in valid_service_codes:
        return
    
    rate_cache[dict_key] = rate_dict

def build_rate_cache_index(rate_cache: dict) -> dict:
    """
    Builds an index from rate_cache keyed by:
    rate_sheet_code -> proc_code -> list of full keys in rate_cache.

    This supports fast lookup of all modifier/POS variations for a given proc_code
    within a specific rate sheet context.
    """
    index = {}

    for full_key in rate_cache:
        # full_key: (rate_sheet_code, proc_code, modifier, pos, code_type)
        rate_sheet_code, proc_code, _, _, _ = full_key

        if rate_sheet_code not in index:
            index[rate_sheet_code] = {}
        if proc_code not in index[rate_sheet_code]:
            index[rate_sheet_code][proc_code] = []

        index[rate_sheet_code][proc_code].append(full_key)

    return index

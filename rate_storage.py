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
    valid_code_key: str = (proc_code, rate_dict["billing_code_type"])
    if valid_code_key not in valid_service_codes:
        return
    
    rate_cache[dict_key] = rate_dict

    if group_key and key_factory and code_tuple:

        rate_sheet_code = group_key.split("#")[0]

        key = key_factory.get_keys_for_rate_sheet(rate_sheet_code).get(group_key)

        if not key:
            # This should only happen if somehow the factory missed this key earlier
            key = RateGroupKey(key=group_key, codes=set(), qualifiers=None)
            key_factory.store[rate_sheet_code][group_key] = key  # <-- nested insert

        key.add_code(proc_code, modifier, pos)

def build_partial_indexes(provider_rates_temp: dict) -> dict[str, dict]:
    """
    Builds partial key indexes for fast lookup of allowed amounts.
    Returns a dictionary with 4 sub-indexes:
      - proc_mod_pos: (proc_code, modifier, pos)
      - proc_mod:     (proc_code, modifier)
      - proc_pos:     (proc_code, pos)
      - proc_only:    proc_code
    """
    indexes = {
        "proc_mod_pos": {},
        "proc_mod": {},
        "proc_pos": {},
        "proc_only": {}
    }

    for key in provider_rates_temp:
        _, proc, mod, pos = key  # Ignore first element (fee_schedule or rate_sheet)

        # (proc, mod, pos)
        indexes["proc_mod_pos"].setdefault((proc, mod, pos), []).append(key)

        # (proc, mod)
        indexes["proc_mod"].setdefault((proc, mod), []).append(key)

        # (proc, pos)
        indexes["proc_pos"].setdefault((proc, pos), []).append(key)

        # (proc,)
        indexes["proc_only"].setdefault(proc, []).append(key)

    return indexes
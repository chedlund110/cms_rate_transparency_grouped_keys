from constants import DEFAULT_EXP_DATE

def store_rate_record(
    ratesheet_temp: dict,
    dict_key: tuple,
    rate_dict: dict
) -> None:
    """
    Stores a rate record for a single rate sheet using a temporary dict.
    """
    ratesheet_temp[dict_key] = rate_dict

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
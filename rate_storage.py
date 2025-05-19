from constants import DEFAULT_EXP_DATE

def store_rate_record(provider_bundle, dict_key, rate_dict, update_proc_xref: bool = True) -> None:
    
    provider_bundle.provider_rates_temp[dict_key] = rate_dict

    # Flush if batch is large
    if len(provider_bundle.rate_write_batch) >= 500:
        provider_bundle.rate_file_writer.write_batch(provider_bundle.rate_write_batch)
        provider_bundle.rate_write_batch.clear()

    # ✅ Optional fallback cache update
    if update_proc_xref:
        proc_code = rate_dict.get("billing_code", "")
        if proc_code and proc_code not in provider_bundle.proc_code_amount_xref:
            provider_bundle.proc_code_amount_xref[proc_code] = {
                "rate": float(rate_dict.get("rate", 0)),
                "percentage": float(rate_dict.get("percentage", 0)),
                "billing_code_type": rate_dict.get("billing_code_type", ""),
                "expiration_date": rate_dict.get("expiration_date", DEFAULT_EXP_DATE),
            }

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
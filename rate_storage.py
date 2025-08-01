from constants import DEFAULT_EXP_DATE
from context import Context
from rate_group_key_factory import RateGroupKeyFactory
from term_bundle import TermBundle
from typing import Optional

def store_rate_record(
    rate_cache: dict,
    dict_key: tuple,
    rate_dict: dict,
    group_key: str = None,
    key_factory: RateGroupKeyFactory = None,
    code_tuple: tuple[str, str, str] = None,
    valid_service_codes: tuple[str, str] = None,
    rate_cache_index: dict = None,
    *,
    term_bundle: TermBundle
) -> None:
    """
    Stores a rate record for a single rate sheet using a temporary dict.
    Also updates the group key with the procedure code used.
    Updates all rate_cache indexes (proc, modifier, pos) if provided.
    """
    
    proc_code, modifier, pos = code_tuple
    billing_code_type = rate_dict["billing_code_type"]

    temp_proc_code = proc_code.zfill(4) if billing_code_type == 'RC' else proc_code
    valid_code_key: str = (temp_proc_code, billing_code_type)

    if valid_code_key not in valid_service_codes:
        return

    # Exclusion logic: allow overwrite only if is_exclusion is True
    existing = rate_cache.get(dict_key)
    if existing and not term_bundle.is_exclusion:
        if existing == rate_dict:
            # Do not overwrite existing rate unless this is an exclusion
            return

    # Inject flags into the rate dict
    rate_dict["was_poa"] = term_bundle.was_poa
    rate_dict["source_term_id"] = term_bundle.term_id

    # Only update indexes for non-POA terms
    if rate_cache_index is not None and not term_bundle.was_poa:
        rate_sheet_code = dict_key[0]

        # Update by_proc
        if "by_proc" in rate_cache_index:
            by_proc = rate_cache_index["by_proc"].setdefault(rate_sheet_code, {})
            by_proc.setdefault(proc_code, set()).add(dict_key)

        # Update by_modifier
        if "by_modifier" in rate_cache_index:
            by_mod = rate_cache_index["by_modifier"].setdefault(rate_sheet_code, {})
            by_mod.setdefault(modifier, set()).add(dict_key)

        # Update by_pos
        if "by_pos" in rate_cache_index:
            by_pos = rate_cache_index["by_pos"].setdefault(rate_sheet_code, {})
            by_pos.setdefault(pos, set()).add(dict_key)

    rate_cache[dict_key] = rate_dict

def build_rate_cache_index(rate_cache: dict) -> dict:
    """
    Builds multiple indexes from rate_cache:
    - by_proc: rate_sheet_code -> proc_code -> list of full keys
    - by_modifier: rate_sheet_code -> modifier -> list of full keys
    - by_pos: rate_sheet_code -> pos -> list of full keys

    These indexes support fast lookups when filtering by different dimensions.
    """
    by_proc = {}
    by_modifier = {}
    by_pos = {}

    for full_key in rate_cache:
        if len(full_key) == 5:
            rate_sheet_code, proc_code, modifier, pos, _ = full_key
        else:
            rate_sheet_code, proc_code, modifier, pos, _, _ = full_key

        # Index by proc
        if rate_sheet_code not in by_proc:
            by_proc[rate_sheet_code] = {}
        if proc_code not in by_proc[rate_sheet_code]:
            by_proc[rate_sheet_code][proc_code] = []
        by_proc[rate_sheet_code][proc_code].append(full_key)

        # Index by modifier
        if rate_sheet_code not in by_modifier:
            by_modifier[rate_sheet_code] = {}
        if modifier not in by_modifier[rate_sheet_code]:
            by_modifier[rate_sheet_code][modifier] = []
        by_modifier[rate_sheet_code][modifier].append(full_key)

        # Index by POS
        if rate_sheet_code not in by_pos:
            by_pos[rate_sheet_code] = {}
        if pos not in by_pos[rate_sheet_code]:
            by_pos[rate_sheet_code][pos] = []
        by_pos[rate_sheet_code][pos].append(full_key)

    return {
        "by_proc": by_proc,
        "by_modifier": by_modifier,
        "by_pos": by_pos
    }

def ensure_rate_cache_index(context: Context, rate_cache: dict):
    if not hasattr(context, "rate_cache_index") or context.rate_cache_index is None:
        context.rate_cache_index = build_rate_cache_index(rate_cache)

def update_rate_cache_index(index: dict, full_key: tuple) -> None:
    """
    Updates the existing rate_cache index by inserting the new full_key
    into the appropriate place if not already present.
    """
    if len(full_key) == 5:
        rate_sheet_code, proc_code, *_ = full_key
    else:
        rate_sheet_code, proc_code, *_ = full_key

    if rate_sheet_code not in index:
        index[rate_sheet_code] = {}

    if proc_code not in index[rate_sheet_code]:
        index[rate_sheet_code][proc_code] = []

    if full_key not in index[rate_sheet_code][proc_code]:
        index[rate_sheet_code][proc_code].append(full_key)
from term_bundle import TermBundle
from rate_group_key_factory import RateGroupKeyFactory, RateGroupKey

def build_rate_group_key_if_needed(
    term_bundle: TermBundle,
    standard_group_key: str,
    key_factory: RateGroupKeyFactory
) -> str:
    """
    Builds and stores the standard group key and optionally a qualified group key
    if provider-level filters exist on the term.

    Returns the appropriate group key (qualified or standard).
    """
    rate_sheet_code = term_bundle.rate_sheet_code
    term_id = term_bundle.term_id

    # Always make sure the standard key is present in the store
    sheet_store = key_factory.store.setdefault(rate_sheet_code, {})
    if standard_group_key not in sheet_store:
        sheet_store[standard_group_key] = RateGroupKey(
            key=standard_group_key,
            qualifiers={}  # No qualifiers for standard key
        )

    provider_ranges = term_bundle.provider_ranges
    if not provider_ranges:
        return standard_group_key

    # Executive decision: use only the first provider range
    first_code_type = next(iter(provider_ranges))
    values_dict = provider_ranges.get(first_code_type)
    if not values_dict:
        return standard_group_key

    qualified_group_key = f"{rate_sheet_code}#{term_id}#{first_code_type}"

    if qualified_group_key not in sheet_store:
        # Separate included and excluded values
        included = []
        excluded = []
        for value, meta in values_dict.items():
            if meta.get("not_logic_ind"):
                excluded.append(value)
            else:
                included.append(value)

        # Build and store the block under the filter section
        filter_section = sheet_store.setdefault(first_code_type, [])
        block = {"group_key": qualified_group_key}
        short_key = first_code_type.replace("CodeTypeProvider", "").lower()
        if included:
            block[short_key] = included
        if excluded:
            block[f"not_{short_key}"] = excluded

        # Store the qualified key as a full RateGroupKey instance
        sheet_store[qualified_group_key] = RateGroupKey(
            key=qualified_group_key,
            qualifiers=block
        )

    return qualified_group_key
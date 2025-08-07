from term_bundle import TermBundle
from rate_group_key_factory import RateGroupKeyFactory, RateGroupKey

def build_rate_group_key_if_needed(
    term_bundle: TermBundle,
    standard_group_key: str,
    key_factory: RateGroupKeyFactory
) -> str:
    rate_sheet_code = term_bundle.rate_sheet_code
    term_id = term_bundle.term_id

    # Always ensure standard group key is present
    sheet_store = key_factory.store.setdefault(rate_sheet_code, {})
    if standard_group_key not in sheet_store:
        sheet_store[standard_group_key] = RateGroupKey(
            key=standard_group_key,
            qualifiers={}
        )

    provider_ranges = term_bundle.provider_ranges
    if not provider_ranges:
        return standard_group_key

    # Use only the first provider range type
    first_code_type = next(iter(provider_ranges))
    values_dict = provider_ranges.get(first_code_type)
    if not values_dict:
        return standard_group_key

    qualified_group_key = f"{rate_sheet_code}#{term_id}#{first_code_type}"

    if qualified_group_key not in sheet_store:
        # ✅ Use sets to ensure unique values
        included = set()
        excluded = set()

        for value, meta in values_dict.items():
            if meta.get("not_logic_ind"):
                excluded.add(value)
            else:
                included.add(value)

        # Build and store the filter block
        filter_section = sheet_store.setdefault(first_code_type, [])
        block = {"group_key": qualified_group_key}
        short_key = first_code_type.replace("CodeTypeProvider", "").lower()

        if included:
            block[short_key] = sorted(included)  # Back to list for JSON/export
        if excluded:
            block[f"not_{short_key}"] = sorted(excluded)

        # ✅ Prevent duplicate blocks under the same filter type
        if block not in filter_section:
            filter_section.append(block)

        # Store the RateGroupKey instance
        sheet_store[qualified_group_key] = RateGroupKey(
            key=qualified_group_key,
            qualifiers=block
        )

    return qualified_group_key
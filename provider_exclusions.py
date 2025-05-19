from context import Context
from provider_bundle import ProviderBundle
import re

def is_valid_provider_value(code_dict: dict[str, dict], value_to_check: str) -> bool:
    if not code_dict:
        return True  # No restriction = allow

    for code_value, logic in code_dict.items():
        not_logic = logic.get("not_logic_ind", False)

        if not_logic:
            if value_to_check == code_value:
                return False  # explicitly excluded
        else:
            if value_to_check == code_value:
                return True  # explicitly included

    # If not found in either case
    has_positive_logic = any(not v.get("not_logic_ind", False) for v in code_dict.values())
    return not has_positive_logic  # if inclusion list exists, and value not in it → reject

def check_provider_level_exclusions(context: Context, provider_bundle: ProviderBundle, code_ranges) -> bool:

    # this routine will check the code ranges for the term
    # we want to check the ones that apply to the provider
    # such as Tax Id, Taxonomy, etc.  The term could be disqualified
    # based on that information.

    provider_ok: bool = True
    if not code_ranges:
        return provider_ok
    for code_range_type in code_ranges:
        match code_range_type:
            case 'CodeTypeServiceProviderTaxID':
                prov_ssn: str = provider_bundle.prov_ssn
                provider_ok: bool = is_valid_provider_value(code_ranges[code_range_type], prov_ssn)
            case 'CodeTypeProviderNPI':
                prov_npi: str = provider_bundle.npi
                provider_ok: bool = is_valid_provider_value(code_ranges[code_range_type], prov_npi)
            case 'CodeTypeProviderID':
                prov_id: str = provider_bundle.provid
                provider_ok: bool = is_valid_provider_value(code_ranges[code_range_type], prov_id)
            case 'CodeTypeProviderTaxonomyCode':
                prov_taxonomy: str = provider_bundle.taxonomy
                provider_ok: bool = is_valid_provider_value(code_ranges[code_range_type], prov_taxonomy)
            case 'CodeTypeProviderType':
                prov_type: str = provider_bundle.provtype
                provider_ok: bool = is_valid_provider_value(code_ranges[code_range_type], prov_type)
            case 'CodeTypeProviderZip':
                    prov_zip: str = provider_bundle.zip
                    # Normalize ZIP range keys to first 5 digits
                    provider_ok = is_valid_provider_value(code_ranges[code_range_type], prov_zip)
            case _:
                pass

        if not provider_ok:
            break

    return provider_ok

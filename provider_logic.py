from typing import Any
from constants import TAXONOMY_ATTRIBUTE_ID
from context import Context
from collections import defaultdict
from locality_matching import attach_provider_locality_info
from provider_bundle import ProviderBundle
from utilities import build_in_clause_from_list, update_prov_grp_contract_keys
from provider_bundle import ProviderBundle
from rate_group_key_factory import RateGroupKeyFactory, RateGroupKey
from shared_config import SharedConfig
from context import Context
from file_writer import write_provider_identifiers_record, write_prov_grp_contract_file

def provider_matches_qualifiers(provider_bundle: ProviderBundle, qualifiers: dict, provider_code_field_map: dict) -> bool:
    field_map = provider_code_field_map

    for qualifier_type, allowed_values in qualifiers.items():
        if not allowed_values:
            continue

        provider_attr_name = field_map.get(qualifier_type)
        if not provider_attr_name:
            continue  # skip unknown qualifier types

        provider_value = getattr(provider_bundle, provider_attr_name, None)
        if provider_value not in allowed_values:
            return False

    return True

def process_single_provider(
        provider_bundle: ProviderBundle,
        group_keys: dict[str, RateGroupKey],
        context: Context,
        shared_config: SharedConfig
    ) -> None:

    # Attach (carrier, locality) match based on provider ZIP + carrier_number
    attach_provider_locality_info(provider_bundle, context)
    provider_code_field_map = shared_config.provider_code_field_map
    for group_key, rgk in group_keys.items():
        # Skip filter blocks like 'CodeTypeProviderTaxonomyCode' (those are lists of blocks)
        if not isinstance(rgk, RateGroupKey):
            continue
        provider_matches_qual = provider_matches_qualifiers(provider_bundle, rgk.qualifiers, provider_code_field_map)
        if not rgk.qualifiers or provider_matches_qual:

            # If the rate sheet requires locality match, and none was found, skip
            rate_sheet_code = group_key.split("#")[0]
            
            if "#locality" in group_key:
                if not getattr(provider_bundle, "locality_key", None):
                    continue  # Skip if no locality match

            update_prov_grp_contract_keys(provider_bundle, group_key)
            write_provider_identifiers_record(context, provider_bundle, group_key)
            write_prov_grp_contract_file(context, provider_bundle, group_key)

    return provider_bundle

def group_provider_rows_by_unique_key(provider_rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for row in provider_rows:
        provid = row.get("provid", "").strip()
        rate_sheet_code = row.get("NxRateSheetId", "").strip()

        if not provid or not rate_sheet_code:
            continue  # skip rows missing critical identifiers

        grouped[(provid, rate_sheet_code)].append(row)

    return grouped

def build_provider_bundle_from_rows(provider_rows: list[dict[str, Any]]) -> ProviderBundle:
    """
    Takes a list of rows for the same provider/rate_sheet_code and returns a single populated ProviderBundle.
    Assumes all rows share the same provid and NxRateSheetId.
    """
    if not provider_rows:
        raise ValueError("Provider rows cannot be empty.")

    base_row = provider_rows[0]
    provider_bundle = ProviderBundle(base_row)

    # Collect all program_ids from the group of rows
    for row in provider_rows:
        program_id = row.get("programid", "").strip()
        if program_id and program_id not in provider_bundle.program_list:
            provider_bundle.program_list.append(program_id)

    return provider_bundle

def fetch_providers(context) -> list[dict[str, Any]]:

    # use this for facility to run standalone:
    # SUBSTRING(NxRateSheetId, 5, 3) IN ({facility_clause}) and
    #facility_indicators: list[str] = [
    #    "ACH", "CAH", "FER", "ICF", "RBF", "PSY", "LTC", "NRF", "SNF", "ASC", "MHC",
    #    "URG", "AMB", "HCC", "HIF", "HSP", "DIA", "DME", "PHC", "DTC", "DTX", "AUD"
    #]
    #facility_clause = ', '.join(f"'{x}'" for x in facility_indicators)
    
    query = f"""
    SELECT
        PROV.provid,
        PROV.npi,
        PROV.ssn,
        PROV2.fedid 'fedid',
        PROV.provtype,
        entity.phyzip AS prov_zip,
        AFF.affiliationid,
        CTR.effdate,
        CTR.termdate,
        CTR.networkid,
        CTR.contractid,
        CTR.programid,
        CTRNX.NxRateSheetId as NxRateSheetId,
        CTR.contracted,
        PA.thevalue AS taxonomy
    FROM affiliation AFF
    LEFT JOIN provider PROV ON PROV.provid = AFF.provid
    LEFT JOIN provider PROV2 ON PROV2.provid = AFF.affiliateid
    LEFT JOIN entity ON entity.entid = PROV.entityid
    LEFT JOIN providerattribute PA ON PROV.provid = PA.provid
    LEFT JOIN contractinfo CTR ON CTR.affiliationid = AFF.affiliationid
    LEFT JOIN ContractNxRateSheet CTRNX ON CTRNX.ContractId = CTR.contractid
    WHERE
        NxRateSheetId IS NOT NULL AND 
        NxRateSheetId LIKE 'AV%' AND 
        NxRateSheetId not like 'Z%' AND  
        NxRateSheetId not like '%-%' AND 
        (GETDATE() BETWEEN CTR.effdate and CTR.termdate) and 
        contracted = 'Y' AND
        PROV.status = 'Active' AND 
        PA.attributeid = '{TAXONOMY_ATTRIBUTE_ID}' AND
        ctr.programid IN {build_in_clause_from_list(context.program_list)}
    """
    return context.qnxt_conn.execute_query_with_columns(query)


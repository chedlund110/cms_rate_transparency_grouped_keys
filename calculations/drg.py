from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from provider_bundle import ProviderBundle
from rate_storage import store_rate_record
from rate_group_key_factory import RateGroupKeyFactory
from rate_group_utilities import build_rate_group_key_if_needed
from term_bundle import TermBundle
from file_writer import write_provider_identifiers_record
from utilities import get_service_code_type, update_prov_grp_contract_keys

def process_drg_weighting(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    
    rate_key = f"{term_bundle.rate_sheet_code}#drg"
    rate_key = build_rate_group_key_if_needed(term_bundle, rate_key, rate_group_key_factory)
    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    fee_type = "negotiated"

    for drg_code, relative_weight, source_type, year in context.shared_config.drg_weights:
        fee = round(base_rate * float(relative_weight),2)
        modifier = ''
        if rate_type_desc == 'institutional':
            pos = '21'
        else:
            pos = '11'
        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": "DRG",
                    "billing_code_type_ver": "10",
                    "billing_code": drg_code,
                    "pos_collection_key": pos,
                    "negotiated_type": fee_type,
                    "rate": str(fee),
                    "modifier": modifier,
                    "billing_class": rate_type_desc,
                    "expiration_date": term_date,
                    "full_term_section_id": section_id,
                    "calc_bean": calc_bean
                }
        code_tuple = (drg_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, drg_code, modifier, pos, "DRG")
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes)

def process_drg_weighting_day_outlier(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    
    rate_key = f"{term_bundle.rate_sheet_code}#drg"
    rate_key = build_rate_group_key_if_needed(term_bundle, rate_key, rate_group_key_factory)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    fee_type = "negotiated"

    for drg_code, relative_weight, source_type, year in context.shared_config.drg_weights:
        fee = round(base_rate * float(relative_weight),2)
        modifier = ''
        if rate_type_desc == 'institutional':
            pos = '21'
        else:
            pos = '11'
        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": "DRG",
                    "billing_code_type_ver": "10",
                    "billing_code": drg_code,
                    "pos_collection_key": pos,
                    "negotiated_type": fee_type,
                    "rate": str(fee),
                    "modifier": modifier,
                    "billing_class": rate_type_desc,
                    "expiration_date": term_date,
                    "full_term_section_id": section_id,
                    "calc_bean": calc_bean
                }
        code_tuple = (drg_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, drg_code, modifier, pos, "DRG")
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes)
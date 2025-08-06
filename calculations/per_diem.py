from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from rate_group_key_factory import RateGroupKeyFactory
from rate_group_utilities import build_rate_group_key_if_needed
from rate_storage import store_rate_record
from term_bundle import TermBundle
from utilities import get_service_code_type


def process_per_diem(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    provider_ranges = term_bundle.provider_ranges
    if not service_mod_pos_list and not provider_ranges:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#per_diem"
    rate_key = build_rate_group_key_if_needed(term_bundle, rate_key, rate_group_key_factory)

    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "per diem"

    for proc_code, modifier, pos, code_type in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": code_type,
            "billing_code_type_ver": "10",
            "billing_code": proc_code,
            "pos_collection_key": pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": term_date,
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        })

        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos, code_type)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes, context.rate_cache_index, term_bundle=term_bundle)


def process_pd_with_max(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    provider_ranges = term_bundle.provider_ranges
    if not service_mod_pos_list and not provider_ranges:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#per_diem"
    rate_key = build_rate_group_key_if_needed(term_bundle, rate_key, rate_group_key_factory)
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "per diem"

    for proc_code, modifier, pos, code_type in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": code_type,
            "billing_code_type_ver": "10",
            "billing_code": proc_code,
            "pos_collection_key": pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": term_date,
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        })

        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos, code_type)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes, context.rate_cache_index, term_bundle=term_bundle)


def process_three_lev_pd(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    provider_ranges = term_bundle.provider_ranges
    if not service_mod_pos_list and not provider_ranges:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#per_diem"
    rate_key = build_rate_group_key_if_needed(term_bundle, rate_key, rate_group_key_factory)
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = max(x for x in [term_bundle.base_rate, term_bundle.per_diem, term_bundle.outlier] if x is not None)
        fee_type = "per diem"

    for proc_code, modifier, pos, code_type in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": code_type,
            "billing_code_type_ver": "10",
            "billing_code": proc_code,
            "pos_collection_key": pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": term_date,
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        })

        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos, code_type)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes, context.rate_cache_index, term_bundle=term_bundle)


def process_pd_five_lv_confine_day(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    provider_ranges = term_bundle.provider_ranges
    if not service_mod_pos_list and not provider_ranges:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#per_diem"
    rate_key = build_rate_group_key_if_needed(term_bundle, rate_key, rate_group_key_factory)
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = max(x for x in [term_bundle.base_rate, term_bundle.base_rate1, term_bundle.base_rate2, term_bundle.per_diem, term_bundle.outlier] if x is not None)
        fee_type = "per diem"

    for proc_code, modifier, pos, code_type in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": code_type,
            "billing_code_type_ver": "10",
            "billing_code": proc_code,
            "pos_collection_key": pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": term_date,
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        })

        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos, code_type)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes, context.rate_cache_index, term_bundle=term_bundle)


def process_pd_with_alos(context: Context, term_bundle: TermBundle, rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    provider_ranges = term_bundle.provider_ranges
    if not service_mod_pos_list and not provider_ranges:
        return

    rate_sheet_code = term_bundle.rate_sheet_code
    rate_key = f"{rate_sheet_code}#per_diem"
    rate_key = build_rate_group_key_if_needed(term_bundle, rate_key, rate_group_key_factory)
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    term_date = DEFAULT_EXP_DATE
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge

    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "per diem"

    for proc_code, modifier, pos, code_type in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": code_type,
            "billing_code_type_ver": "10",
            "billing_code": proc_code,
            "pos_collection_key": pos,
            "negotiated_type": fee_type,
            "rate": str(fee),
            "modifier": modifier,
            "billing_class": rate_type_desc,
            "expiration_date": term_date,
            "full_term_section_id": section_id,
            "calc_bean": calc_bean
        })

        code_tuple = (proc_code, modifier, pos)
        dict_key = (term_bundle.rate_sheet_code, proc_code, modifier, pos, code_type)
        store_rate_record(rate_cache, dict_key, rate_dict, rate_key, rate_group_key_factory, code_tuple, context.shared_config.valid_service_codes, context.rate_cache_index, term_bundle=term_bundle)

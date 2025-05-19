from context import Context
from constants import DEFAULT_EXP_DATE, rate_template
from provider_bundle import ProviderBundle
from rate_storage import store_rate_record
from term_bundle import TermBundle
from file_writer import write_provider_identifiers_record
from utilities import get_service_code_type, update_prov_grp_contract_keys

def process_per_item(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{provider_bundle.provid}#{provider_bundle.rate_sheet_code}#per_item"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge
    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (provider_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(provider_bundle, dict_key, rate_dict)

def process_unit_ltd_by_chg(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{provider_bundle.provid}#{provider_bundle.rate_sheet_code}#per_unit"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge
    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (provider_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(provider_bundle, dict_key, rate_dict)

def process_percent_plus_excess(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{provider_bundle.provid}#{provider_bundle.rate_sheet_code}#per_unit"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    base_pct_of_charge = term_bundle.base_pct_of_charge
    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (provider_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(provider_bundle, dict_key, rate_dict)

def process_visit_plus_rate_per_hour(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{provider_bundle.provid}#{provider_bundle.rate_sheet_code}#per_item"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.base_rate
    base_pct_of_charge = term_bundle.base_pct_of_charge
    if base_pct_of_charge > 0:
        fee = base_pct_of_charge * 100
        fee_type = "percentage"
    else:
        fee = base_rate
        fee_type = "negotiated"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'

        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (provider_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(provider_bundle, dict_key, rate_dict)

def process_flat_dollar_discount(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle) -> None:

    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{provider_bundle.provid}#{provider_bundle.rate_sheet_code}#per_item"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    fee_schedule_name = term_bundle.fee_schedule_name
    term_date = "99991231"
    base_rate = term_bundle.per_diem
    base_pct_of_charge = term_bundle.base_pct_of_charge
    if not base_pct_of_charge:
        base_pct_of_charge = 1.0
    fee = base_pct_of_charge * 100
    fee_type = "percentage"

    for proc_code, modifier, pos in service_mod_pos_list:
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
            else:
                pos = '11'
                
        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(proc_code),
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
                }

        dict_key = (provider_bundle.rate_sheet_code, proc_code, modifier, pos)
        store_rate_record(provider_bundle, dict_key, rate_dict)

def process_ndc(context: Context, provider_bundle: ProviderBundle, term_bundle: TermBundle):
    service_mod_pos_list = term_bundle.service_mod_pos_list or []
    if not service_mod_pos_list:
        return

    rate_key = f"{provider_bundle.provid}#{provider_bundle.rate_sheet_code}#ndc"
    write_provider_identifiers_record(context, provider_bundle, rate_key)
    update_prov_grp_contract_keys(provider_bundle, rate_key)

    # Static values from term
    calc_bean = term_bundle.calc_bean
    section_id = term_bundle.section_id
    rate_type_desc: str = term_bundle.rate_type_desc
    term_date = "99991231"
    base_pct_of_charge = term_bundle.base_pct_of_charge
    fee_type = "negotiated"

    modifier = ''
    pos = '11'
    for ndc_code, unit_price in context.shared_config.ndc_codes.items():
        fee = unit_price
        if base_pct_of_charge:
            fee = round(base_pct_of_charge * fee,2)
        if pos == '' or pos == '11':
            if rate_type_desc == 'institutional':
                pos = '21'
        rate_dict = {
                    "update_type": "A",
                    "insurer_code": context.insurer_code,
                    "prov_grp_contract_key": rate_key,
                    "negotiation_arrangement": "ffs",
                    "billing_code_type": get_service_code_type(ndc_code),
                    "billing_code_type_ver": "10",
                    "billing_code": ndc_code,
                    "pos_collection_key": pos,
                    "negotiated_type": fee_type,
                    "rate": str(fee),
                    "modifier": modifier,
                    "billing_class": rate_type_desc,
                    "expiration_date": term_date,
                    "full_term_section_id": section_id,
                    "calc_bean": calc_bean
                }

        dict_key = (provider_bundle.rate_sheet_code, ndc_code, modifier, pos)
        store_rate_record(provider_bundle, dict_key, rate_dict)
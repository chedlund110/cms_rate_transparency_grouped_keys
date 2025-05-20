from constants import GROUPER_COLUMN_MAP, rate_template
from context import Context
from rate_storage import store_rate_record
from term_bundle import TermBundle

def calc_asc_grouper_9lv_no_disc(context: Context, term_bundle: TermBundle, rate_cache: dict):
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    rate_sheet_code = term_bundle.rate_sheet_code
    base_pct_of_charge = term_bundle.base_pct_of_charge

    rate_key = f"{rate_sheet_code}#grouper"
    modifier = ''
    pos = '21'
    proc_code_type = "CPT"
    term_date = "99991231"

    amb_surg_codes = context.shared_config.amb_surg_codes
    term_year_applied: str = term_bundle.code_low
    term_source_type: str = term_bundle.code_high

    for (proc_code, source_type, year_applied), group_no in amb_surg_codes.items():
        if source_type != term_source_type or year_applied != term_year_applied:
             continue
        field_name: str = GROUPER_COLUMN_MAP.get(group_no, '')
        fee = term_bundle.term.get(field_name, 0)
        fee_type = "negotiated"

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": proc_code_type,
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

        dict_key = (fee_schedule_name, proc_code, modifier, pos)
        store_rate_record(rate_cache, dict_key, rate_dict)

def calc_asc_grouper_base(context: Context, term_bundle: TermBundle, rate_cache: dict):
    fee_schedule_name = term_bundle.fee_schedule_name
    section_id = term_bundle.section_id
    rate_type_desc = term_bundle.rate_type_desc
    calc_bean = term_bundle.calc_bean
    rate_sheet_code = term_bundle.rate_sheet_code
    base_pct_of_charge = term_bundle.base_pct_of_charge

    rate_key = f"{rate_sheet_code}#grouper"
    modifier = ''
    pos = '21'
    proc_code_type = "CPT"
    term_date = "99991231"

    amb_surg_codes = context.shared_config.amb_surg_codes
    for (proc_code, source_type, year_applied), group_no in amb_surg_codes.items():
        field_name: str = GROUPER_COLUMN_MAP.get(group_no, '')
        fee = term_bundle.base_rate
        fee_type = "negotiated"

        rate_dict = rate_template.copy()
        rate_dict.update({
            "update_type": "A",
            "insurer_code": context.insurer_code,
            "prov_grp_contract_key": rate_key,
            "negotiation_arrangement": "ffs",
            "billing_code_type": proc_code_type,
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

        dict_key = (fee_schedule_name, proc_code, modifier, pos)
        store_rate_record(rate_cache, dict_key, rate_dict)

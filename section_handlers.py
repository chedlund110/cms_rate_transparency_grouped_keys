from context import Context
from provider_bundle import ProviderBundle
from rate_group_key_factory import RateGroupKeyFactory
from term_bundle import TermBundle
from term_handler import process_term

def process_inpatient_case_rate(context: Context, inpatient_case_rate: list[dict], rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
        rate_type_desc = 'institutional'
        for term in inpatient_case_rate:
            subterms = term.get("subterms",{})
            if subterms:
                for subterm in subterms:
                    term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                    process_term(context, term_bundle, rate_cache, rate_group_key_factory)
            else:
                term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
                process_term(context, term_bundle, rate_cache, rate_group_key_factory)
    
def process_inpatient_per_diem(context: Context, inpatient_per_diem: list[dict], rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_type_desc = 'institutional'
    for term in inpatient_per_diem:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, term_bundle, rate_cache, rate_group_key_factory)
        else:
            term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
            process_term(context, term_bundle, rate_cache, rate_group_key_factory)

def process_inpatient_services(context: Context, inpatient_services: list[dict], rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_type_desc = 'institutional'
    for term in inpatient_services:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, term_bundle, rate_cache, rate_group_key_factory)
        else:
            term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
            process_term(context, term_bundle, rate_cache, rate_group_key_factory)

def process_inpatient_exclusions(context: Context, inpatient_exclusions: list[dict], rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_type_desc = 'institutional'
    for term in inpatient_exclusions:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, term_bundle, rate_cache, rate_group_key_factory)
        else:
            term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
            process_term(context, term_bundle, rate_cache, rate_group_key_factory)

def process_outpatient_case_rate(context: Context, outpatient_case_rate: list[dict], rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_type_desc = 'professional'
    for term in outpatient_case_rate:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, term_bundle, rate_cache, rate_group_key_factory)
        else:
            term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
            process_term(context, term_bundle, rate_cache, rate_group_key_factory)
    
def process_outpatient_per_diem(context: Context, outpatient_per_diem: list[dict], rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_type_desc = 'professional'
    for term in outpatient_per_diem:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, term_bundle, rate_cache, rate_group_key_factory)
        else:
            term_bundle = TermBundle(term)
            process_term(context, term_bundle, rate_cache, rate_group_key_factory)

def process_outpatient_services(context: Context, outpatient_services: list[dict], rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_type_desc = 'professional'
    for term in outpatient_services:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, term_bundle, rate_cache, rate_group_key_factory)
        else:
            term_bundle = TermBundle(term)
            process_term(context, term_bundle, rate_cache, rate_group_key_factory)

def process_outpatient_exclusions(context: Context, outpatient_exclusions: list[dict], rate_cache: dict, rate_group_key_factory: RateGroupKeyFactory) -> None:
    rate_type_desc = 'professional'
    for term in outpatient_exclusions:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, term_bundle, rate_cache, rate_group_key_factory)
        else:
            term_bundle = TermBundle(term)
            process_term(context, term_bundle, rate_cache, rate_group_key_factory)



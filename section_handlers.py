from context import Context
from provider_bundle import ProviderBundle
from term_bundle import TermBundle
from term_handler import process_term

def process_inpatient_case_rate(context: Context, provider_bundle: ProviderBundle, inpatient_case_rate: list[dict]) -> None:
        rate_type_desc = 'institutional'
        for term in inpatient_case_rate:
            subterms = term.get("subterms",{})
            if subterms:
                for subterm in subterms:
                    term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                    process_term(context, provider_bundle, term_bundle)
            else:
                term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
                process_term(context, provider_bundle, term_bundle)
    
def process_inpatient_per_diem(context: Context, provider_bundle: ProviderBundle, inpatient_per_diem: list[dict]) -> None:
    rate_type_desc = 'institutional'
    for term in inpatient_per_diem:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, provider_bundle, term_bundle)
        else:
            term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
            process_term(context, provider_bundle, term_bundle)

def process_inpatient_services(context: Context, provider_bundle: ProviderBundle, inpatient_services: list[dict]) -> None:
    rate_type_desc = 'institutional'
    for term in inpatient_services:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, provider_bundle, term_bundle)
        else:
            term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
            process_term(context, provider_bundle, term_bundle)

def process_inpatient_exclusions(context: Context, provider_bundle: ProviderBundle, inpatient_exclusions: list[dict]) -> None:
    rate_type_desc = 'institutional'
    for term in inpatient_exclusions:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, provider_bundle, term_bundle)
        else:
            term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
            process_term(context, provider_bundle, term_bundle)

def process_outpatient_case_rate(context: Context, provider_bundle: ProviderBundle, outpatient_case_rate: list[dict]) -> None:
    rate_type_desc = 'institutional'
    for term in outpatient_case_rate:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm, rate_type_desc=rate_type_desc)
                process_term(context, provider_bundle, term_bundle)
        else:
            term_bundle = TermBundle(term, rate_type_desc=rate_type_desc)
            process_term(context, provider_bundle, term_bundle)
    
def process_outpatient_per_diem(context: Context, provider_bundle: ProviderBundle, outpatient_per_diem: list[dict]) -> None:
    rate_type = 'non_fac'
    for term in outpatient_per_diem:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm)
                process_term(context, provider_bundle, term_bundle)
        else:
            term_bundle = TermBundle(term)
            process_term(context, provider_bundle, term_bundle)

def process_outpatient_services(context: Context, provider_bundle: ProviderBundle, outpatient_services: list[dict]) -> None:
    rate_type: str = 'non-fac'
    for term in outpatient_services:
        subterms = term.get("subterms", {})
        terms_to_process = subterms if subterms else [term]
        for this_term in terms_to_process:
            term_bundle = TermBundle(this_term)
            process_term(context, provider_bundle, term_bundle)

def process_outpatient_exclusions(context: Context, provider_bundle: ProviderBundle, outpatient_exclusions: list[dict]) -> None:
    rate_type = 'non_fac'
    for term in outpatient_exclusions:
        subterms = term.get("subterms",{})
        if subterms:
            for subterm in subterms:
                term_bundle = TermBundle(subterm)
                process_term(context, provider_bundle, term_bundle)
        else:
            term_bundle = TermBundle(term)
            process_term(context, provider_bundle, term_bundle)



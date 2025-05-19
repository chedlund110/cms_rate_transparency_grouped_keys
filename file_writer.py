from constants import rate_template
from constants import FIELD_DELIM
from context import Context
from provider_bundle import ProviderBundle
from utilities import get_dict_value
import time

def open_writer(file_path: str):
    return open(file_path, "w", encoding="utf-8")

def write_provider_identifiers_record(
    context: Context,
    provider_bundle: ProviderBundle,
    rate_id: str
) -> None:
    
    if not rate_id or rate_id in provider_bundle.provider_rate_keys:
        return

    provider_bundle.provider_rate_keys.append(rate_id)

    fields = [
        "A",
        context.insurer_code,
        provider_bundle.provid,
        provider_bundle.provid,
        provider_bundle.fedid,
        provider_bundle.npi,
        rate_id,
        "99991231",
        provider_bundle.provid
    ]
    
    output_rec = FIELD_DELIM.join(fields) + '\n'

    # Use the open file handle from context
    context.provider_identifier_output_file.write(output_rec)

def write_prov_grp_contract_file(context: Context, provider_bundle: ProviderBundle) -> None:
    f = context.prov_grp_contract_output_file

    for xref_key, xref_details in provider_bundle.prov_grp_contract_keys.items():
        programs: set = xref_details.get("programs", set())
        rate_keys: set = xref_details.get("rate_keys", set())

        for program_id in programs:
            for rate_key in rate_keys:
                if rate_key:
                    data_fields = [context.insurer_code, program_id, rate_key]
                    output_rec = FIELD_DELIM.join(data_fields) + '\n'
                    f.write(output_rec)

def write_rate_records(context: Context, provider_bundle: ProviderBundle) -> None:
    for rate_tuple, serv_dict in provider_bundle.provider_rates_temp.items():
        context.rate_file_writer.write(serv_dict)


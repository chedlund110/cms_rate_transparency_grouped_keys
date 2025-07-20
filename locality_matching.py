from context import Context
from provider_bundle import ProviderBundle

def find_matching_locality(provider_zip: str, carrier_number: str, context: Context) -> tuple[str, str] | None:
    """
    Find the matching (carrier, locality) tuple for the provider's ZIP code.
    """
    try:
        zip_int = int(provider_zip)
    except ValueError:
        return None  # Invalid ZIP

    for carrier, locality, begin_zip, end_zip in context.locality_zip_ranges:
        if carrier != carrier_number:
            continue
        try:
            if int(begin_zip) <= zip_int <= int(end_zip):
                return (carrier, locality)
        except ValueError:
            continue  # Skip invalid zip ranges

    return None  # No match found

def attach_provider_locality_info(bundle: ProviderBundle, context: Context) -> None:
    """
    Attach the matching locality info (carrier, locality) to the provider bundle, if found.
    """
    zip_code = getattr(bundle, "zip_code", None)
    carrier_number = getattr(bundle, "carrier_number", None)

    if not zip_code or not carrier_number:
        return  # Required info missing

    result = find_matching_locality(zip_code, carrier_number, context)

    if result:
        bundle.locality_key = result  # You can choose your attribute name here
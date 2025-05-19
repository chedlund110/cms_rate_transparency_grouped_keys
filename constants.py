FIELD_DELIM = "|"
MAX_FILE_SIZE = 5_000_000_000
TAXONOMY_ATTRIBUTE_ID = 'DMSC000147824'
DEFAULT_EXP_DATE = "99991231"

rate_template = {
    "update_type": "A",
    "insurer_code": None,
    "prov_grp_contract_key": None,
    "negotiation_arrangement": None,
    "billing_code_type": None,
    "billing_code_type_ver": "10",
    "billing_code": None,
    "covered_bundle_key": '',
    "pos_collection_key": None,
    "negotiated_type": None,
    "rate": None,
    "modifier": None,
    "billing_class": None,
    "expiration_date": None
}

section_mapping = {
    1: "preprocessing",
    2: "inpatient exclusions",
    3: "inpatient case rate",
    4: "inpatient per diem",
    5: "inpatient services",
    6: "inpatient stop loss",
    7: "outpatient exclusions",
    8: "outpatient case rate",
    9: "outpatient per diem",
    10: "outpatient services",
    11: "outpatient stop loss",
    12: "post processing"
}
section_rate_types = {
    1:"faclity",
    2:"facility",
    3:"facility",
    4:"facility",
    5:"facility",
    6:"facility",
    7:"non_facility",
    8:"non_facility",
    9:"non_facility",
    10:"non_facility",
    11:"non_facility",
    12:"non_facility"
}

GROUPER_COLUMN_MAP = {
    1: "BASERATE",
    2: "BASERATE1",
    3: "BASERATE2",
    4: "BASEPERCENTOFCHGS",
    5: "PERDIEM",
    6: "SECONDARYPERCENTOFCHGS",
    7: "OTHERPERCENTOFCHGS",
    8: "OTHERPERCENTOFCHGS1",
    9: "OUTLIER"
}
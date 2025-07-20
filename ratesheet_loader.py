import logging
from context import Context
from constants import section_mapping
from shared_config import SharedConfig
from typing import Any

def load_ratesheet(context: Context, query: str, rate_sheet_code: str = None) -> dict[str, list[dict[str, Any]]]:
    rate_sheet = {
        "preprocessing": [],
        "inpatient exclusions": [],
        "inpatient case rate": [],
        "inpatient per diem": [],
        "inpatient services": [],
        "inpatient stop loss": [],
        "outpatient exclusions": [],
        "outpatient case rate": [],
        "outpatient per diem": [],
        "outpatient services": [],
        "outpatient stop loss": [],
        "post processing": []
    }

    rate_sheet_terms = context.networx_conn.execute_query_with_columns(query)

    for term in rate_sheet_terms:

        if rate_sheet_code is not None:
            term["RATESHEETCODE"] = rate_sheet_code

        section_number = term.get("DISPLAYSECTIONNUMBER")
        if section_number is None:
            continue

        section_name = section_mapping.get(section_number, "")
        if not section_name:
            continue

        sub_rate_sheet_id = term.get("SUBRATESHEETID")
        sub_rate_sheet_ind = term.get("SUBRATESHEETIND")

        term["subterms"] = []
        if sub_rate_sheet_id and sub_rate_sheet_ind == 0:
            term["subterms"] = load_subterms(context, sub_rate_sheet_id, term)
        else:
            if term["CALCBEAN"] == 'CalcOptumPhysicianPricer':
                term["subterms"] = load_subterms(context, '4649', term)

        rate_sheet[section_name].append(term)

    return rate_sheet

def load_ratesheet_by_code(context: Context, rate_sheet_code: str) -> dict[str, list[dict[str, Any]]]:
    query = f"""
    SELECT 
        SRST.RATESHEETID, SRST.CALCBEAN, SRST.ACTIONPARM1, SRST.BASEPERCENTOFCHGS, SRST.CODEGROUPID,
        SRST.CODELOWVALUE, SRST.CODEHIGHVALUE, SRST.CODETYPEBEAN, SRST.DISPLAYSECTIONNUMBER,
        SRST.SEQNUMBER, SRST.DISABLED, SRST.RATESHEETTERMID, SRST.SUBRATESHEETID, SRS.SUBRATESHEETIND,
        SRST.BASERATE, SRST.BASERATE1, SRST.BASERATE2, SRST.PERDIEM, SRST.USERFIELD1,
        SRST.SECONDARYPERCENTOFCHGS, SRST.OTHERPERCENTOFCHGS, SRST.OTHERPERCENTOFCHGS1,
        SRST.OUTLIER, SRST.OUTLIERPERCENTAGE
    FROM STDRATESHEETS SRS
    LEFT JOIN STDRATESHEETTERMS SRST ON SRS.RATESHEETID = SRST.RATESHEETID
    WHERE SRS.RATESHEETCODE = '{rate_sheet_code}'
      AND GETDATE() BETWEEN SRST.FROMDATE AND SRST.TODATE
    """
    return load_ratesheet(context, query, rate_sheet_code)

def load_ratesheet_by_id(context: Context, rate_sheet_id: int) -> dict[str, list[dict[str, Any]]]:
    query = f"""
    SELECT 
        SRST.RATESHEETID, SRST.CALCBEAN, SRST.ACTIONPARM1, SRST.BASEPERCENTOFCHGS, SRST.CODEGROUPID,
        SRST.CODELOWVALUE, SRST.CODEHIGHVALUE, SRST.CODETYPEBEAN, SRST.DISPLAYSECTIONNUMBER,
        SRST.SEQNUMBER, SRST.DISABLED, SRST.RATESHEETTERMID, SRST.SUBRATESHEETID, SRS.SUBRATESHEETIND,
        SRST.BASERATE, SRST.BASERATE1, SRST.BASERATE2, SRST.PERDIEM, SRST.USERFIELD1,
        SRST.SECONDARYPERCENTOFCHGS, SRST.OTHERPERCENTOFCHGS, SRST.OTHERPERCENTOFCHGS1,
        SRST.OUTLIER, SRST.OUTLIERPERCENTAGE
    FROM STDRATESHEETS SRS
    LEFT JOIN STDRATESHEETTERMS SRST ON SRS.RATESHEETID = SRST.RATESHEETID
    WHERE SRS.RATESHEETID = '{rate_sheet_id}'
      AND GETDATE() BETWEEN SRST.FROMDATE AND SRST.TODATE
    """
    return load_ratesheet(context, query)

def load_subterms(context: Context, rate_sheet_id: str, term: dict) -> list[dict[str, Any]]:
    if not hasattr(context, "subratesheet_cache"):
        context.subratesheet_cache = {}

    if rate_sheet_id in context.subratesheet_cache:
        return context.subratesheet_cache[rate_sheet_id]

    rate_sheet_code: str = term.get("RATESHEETCODE", "")

    query = f"""
    SELECT 
        SRST.CALCBEAN, SRST.ACTIONPARM1, SRST.BASEPERCENTOFCHGS, SRST.CODEGROUPID,
        SRST.CODELOWVALUE, SRST.CODEHIGHVALUE, SRST.CODETYPEBEAN, SRST.DISPLAYSECTIONNUMBER,
        SRST.SEQNUMBER, SRST.DISABLED, SRST.RATESHEETTERMID, SRST.SUBRATESHEETID, SRS.SUBRATESHEETIND,
        SRST.BASERATE, SRST.BASERATE1, SRST.BASERATE2, SRST.PERDIEM, SRST.USERFIELD1,
        SRST.SECONDARYPERCENTOFCHGS, SRST.OTHERPERCENTOFCHGS, SRST.OTHERPERCENTOFCHGS1,
        SRST.OUTLIER, SRST.OUTLIERPERCENTAGE
    FROM STDRATESHEETS SRS
    LEFT JOIN STDRATESHEETTERMS SRST ON SRS.RATESHEETID = SRST.RATESHEETID
    WHERE SRS.RATESHEETID = '{rate_sheet_id}'
      AND GETDATE() BETWEEN SRST.FROMDATE AND SRST.TODATE
    """
    rate_sheet_terms = context.networx_conn.execute_query_with_columns(query)
    subterms = []

    parent_section_number = int(term.get("DISPLAYSECTIONNUMBER") or 0)
    parent_seq_number = int(term.get("SEQNUMBER") or 0)

    for subterm in rate_sheet_terms:
        if rate_sheet_code:
            subterm["RATESHEETCODE"] = rate_sheet_code
        subterm["PARENTSECTIONNUMBER"] = parent_section_number
        subterm["PARENTSEQNUMBER"] = parent_seq_number
        subterms.append(subterm)

    context.subratesheet_cache[rate_sheet_id] = subterms
    return subterms


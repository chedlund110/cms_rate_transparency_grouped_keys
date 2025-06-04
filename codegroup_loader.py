from collections import defaultdict
from context import Context
from utilities import get_dict_value

def is_valid_cpt4(code: str) -> bool:
    return str(code).isdigit() and len(code) in (4, 5)

def is_valid_place_of_service(code: str) -> bool:
    return str(code).isdigit() and len(code) <= 2

def default_code_group_entry():
    return {
        "code_group_name": None,
        "values": []
    }


def load_code_groups(networx_conn) -> dict:
    query = """
    SELECT
        cg.CODEGROUPID,
        cg.CODEGROUPNAME,
        cgv.CODELOWVALUE,
        cgv.CODEHIGHVALUE,
        cgv.CODETYPEBEAN,
        cgv.NESTEDCODEGROUPID,
        cgv.NOTLOGICIND
    FROM CODEGROUPS cg
    JOIN CODEGROUPVALUES cgv ON cg.CODEGROUPID = cgv.CODEGROUPID
    WHERE GETDATE() BETWEEN cgv.CODEVALUESEFFDATE AND cgv.CODEVALUESTERMDATE
    ORDER BY cg.CODEGROUPID, cgv.SEQNUMBER
    """

    rows = networx_conn.execute_query_with_columns(query)
    
    code_groups = defaultdict(default_code_group_entry) 
    
    for row in rows:
        group_id = row.get("CODEGROUPID", 0)
        group_name = row.get("CODEGROUPNAME", '')
        code_groups[group_id]["code_group_name"] = group_name

        code_groups[group_id]["values"].append({
            "code_low": row.get("CODELOWVALUE", ''),
            "code_high": row.get("CODEHIGHVALUE", ''),
            "code_type": row.get("CODETYPEBEAN", ''),
            "nested_code_group_id": row.get("NESTEDCODEGROUPID", 0),
            "not_logic_ind": row.get("NOTLOGICIND", 0)
        })

    return code_groups

def load_ambsurg_codes(conn) -> dict:
    query = """
    SELECT AMBSURGGRPCODE, ASCGROUPNUMBER, SOURCETYPE, YEARAPPLIED
    FROM AMBSURGGRPCODES
    """
    rows = conn.execute_query_with_columns(query)
    return {
    (row["AMBSURGGRPCODE"], row["SOURCETYPE"], row["YEARAPPLIED"]): int(row["ASCGROUPNUMBER"])
    for row in rows
    if row.get("AMBSURGGRPCODE") and row.get("SOURCETYPE") and row.get("YEARAPPLIED") is not None and row.get("ASCGROUPNUMBER") is not None
}

def load_ndc_codes(conn) -> dict:
    query = """
    SELECT NDCCODE, UNITPRICE
    FROM NDCPRICING
    """
    rows = conn.execute_query_with_columns(query)
    return {
    row["NDCCODE"]:row["UNITPRICE"]
    for row in rows
    if row.get("NDCCODE",'') and row.get("UNITPRICE",'') is not None
}

def load_drg_weights(conn) -> dict:
    query = """
    SELECT DRG, RELATIVEWEIGHT, SOURCETYPE, YEARAPPLIED, EFFECTIVEDATE, TERMINATIONDATE 
    FROM DRGWEIGHTS 
    WHERE GETDATE() BETWEEN EFFECTIVEDATE AND TERMINATIONDATE
    """
    rows = conn.execute_query_with_columns(query)
    return {
    (row["DRG"], row["RELATIVEWEIGHT"], row["SOURCETYPE"], row["YEARAPPLIED"])
    for row in rows
    if row.get("DRG") and row.get("RELATIVEWEIGHT") and row.get("SOURCETYPE") and row.get("YEARAPPLIED")
}
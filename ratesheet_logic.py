# ratesheet_logic.py

def fetch_ratesheets(context) -> list[dict]:
    # pull rate sheet–driven data
    # RATESHEETCODE = 'AVCRPRF00221' AND 
    # AVCRPRF00336
    # RATESHEETCODE = 'AVCRPRF00417' AND 
    # AVCRPRF00026
    # RATESHEETCODE = 'AVCRPRF00026' AND 
    query: str = f"""
    SELECT * 
    FROM STDRATESHEETS
    WHERE
        RATESHEETCODE = 'AVCRPRF00002' AND 
        RATESHEETCODE IS NOT NULL AND 
        RATESHEETCODE LIKE 'AV%' AND 
        RATESHEETCODE NOT LIKE 'AVGB%' AND 
        RATESHEETCODE not like 'Z%' AND  
        RATESHEETCODE not like '%-%'
    """
    return context.networx_conn.execute_query_with_columns(query)
    
def group_rows_by_ratesheet_id(rows: list[dict]) -> dict[str, list[dict]]:
    grouped = {}
    for row in rows:
        key = row["RATESHEETID"]
        grouped.setdefault(key, []).append(row)
    return grouped
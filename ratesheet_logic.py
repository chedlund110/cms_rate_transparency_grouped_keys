# ratesheet_logic.py

def fetch_ratesheets(context) -> list[dict]:
    # Your SQL or logic to pull rate sheet–driven data
    query: str = f"""
    SELECT * 
    FROM STDRATESHEETS
    WHERE
        RATESHEETCODE = 'AVCRPRF00221' AND 
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
from typing import Any

class DRGCodeExtract:

    def __init__(self, db_conn, output_file):
        self.conn = db_conn
        self.output_file = output_file

    def extract_data(self) -> tuple[int,set]:
        rec_cnt: int = 0
        valid_service_codes = set()
        query: str = "SELECT codeid, description FROM drgcode"
        results = self.conn.execute_query_with_columns(query)
        for row in results:
            code_id: str = str(row.get("codeid","").strip())
            line = '|'.join([code_id,"DRG","10",str(row.get("description", "")).strip(),""]) + '\n'
            self.output_file.write(line)
            rec_cnt += 1
            valid_service_codes.add((row.get("codeid",""),"DRG"))
        return rec_cnt, valid_service_codes
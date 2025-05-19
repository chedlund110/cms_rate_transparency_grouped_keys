from typing import Any

class DRGCodeExtract:

    def __init__(self, db_conn, output_file):
        self.conn = db_conn
        self.output_file = output_file

    def extract_data(self) -> int:
        rec_cnt: int = 0
        query: str = "SELECT codeid, description FROM drgcode"
        results = self.conn.execute_query_with_columns(query)
        for row in results:
            line = '|'.join([str(row.get("codeid", "")).strip(),"DRG","10",str(row.get("description", "")).strip(),""]) + '\n'
            self.output_file.write(line)
            rec_cnt += 1
        return rec_cnt
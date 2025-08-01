from typing import Any

class ProcCodeExtract:

    def __init__(self, db_conn, output_file):
        self.conn = db_conn
        self.output_file = output_file

    def extract_data(self) -> tuple[int,set]:
        rec_cnt: int = 0
        valid_service_codes: set = set()
        query: str = "SELECT pcode, description FROM proccode"
        results: list[dict[str,Any]] = self.conn.execute_query_with_columns(query)
        row: str = ""
        for row in results:
            proc_code: str = str(row.get("pcode",'').strip())
            if proc_code[0].isalpha():
                code_type = "HCPCS"
            else:
                code_type = "CPT"

            line = '|'.join([proc_code,code_type,"10",str(row.get("description", "")).strip(),""]) + '\n'
            self.output_file.write(line)
            rec_cnt += 1
            valid_service_codes.add((proc_code, code_type))
        return rec_cnt, valid_service_codes
            
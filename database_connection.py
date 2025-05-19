import pyodbc
from typing import Any

class DatabaseConnection:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.conn = None

    def connect(self) -> None:
        try:
            self.conn = pyodbc.connect(self.connection_string)
        except Exception as e:
            raise

    def disconnect(self) -> None:
        if self.conn:
            self.conn.close()

    def execute_query(self,query) -> None:
        try:
            if self.conn == None:
                self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            raise

    def execute_query_with_columns(self,query) -> list[dict[str,Any]]:
        try:
            if self.conn == None:
                self.connect()
            cursor = self.conn.cursor()
            cursor.execute(query)
            columns = [column[0] for column in cursor.description]
            results = cursor.fetchall()
            json_data = []
            for row in results:
                row_dict = dict(zip(columns, row))
                json_data.append(row_dict)
            return json_data
        except Exception as e:
            raise
        finally:
            self.disconnect

    def fetch_one(self, query, params=None) -> list[dict[str,Any]]:

        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchone()
        except Exception as e:
            raise

    def fetch_all(self, query, params=None) -> list[dict[str,Any]]:
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            raise

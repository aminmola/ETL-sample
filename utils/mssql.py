import pyodbc
from typing import List

from sqlalchemy_utils import table_name

import utils.config as cfg
from utils.logger import Logger

log = Logger("sale")


class MsSQL(object):
    """
    mssql connection
    """

    def __init__(self,db_name: str,):
        """
        Initialize mssql class
        """
        self._connection = None
        self.database_name = db_name


    def __enter__(self):
        """
        Open mssql connection
        """
        try:
            self._connection = pyodbc.connect(f'DRIVER=FreeTDS;'
                                              f'SERVER={cfg.SQL_SERVER};'
                                              f'PORT={cfg.SQL_PORT};'
                                              f'DATABASE={self.database_name};'
                                              f'UID={cfg.SQL_USERNAME};'
                                              f'PWD={cfg.SQL_PASSWORD};'
                                              f'TDS_VERSION=8.0;')
            return self
        except Exception as e:
            print(f"Can not connect to mssql, {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        close mssql connection
        """
        try:
            self._connection.close()
        except Exception as e:
            print(f"mssql close connection failed, {str(e)}")

    def execute(self, query: str) -> bool:
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            cursor.close()
            self._connection.commit()
            return True
        except Exception as e:
            print(f"mssql execute query failed, {str(e)}")
            return False

    def read(self, query):
        try:
            cursor = self._connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()
            self._connection.commit()
            return records
        except Exception as e:
            print(f"mssql execute query failed, {str(e)}")
            return None

    def insert(self, table: str, record: dict or List[dict]) -> bool:
        """
        Insert record in table
        """
        try:
            fields = ",".join(record.keys())
            values = tuple(record.values())
            query = f"INSERT INTO {table} ({fields}) VALUES {values}"
            if len(values) == 1:
                query = query.replace(",", "")
            cursor = self._connection.cursor()
            cursor.execute(query)
            cursor.close()
            self._connection.commit()
            return True
        except Exception as e:
            print(f"Insert {table} failed, {str(e)}")
            return False

    def update(self, table: str, record: dict or List[dict], condition: str) -> bool:
        """
        Update record in table
        """
        try:
            field_values = []
            for field, value in record.items():
                if isinstance(value, str):
                    value = f"'{value}'"
                field_values.append(f"{field}={value}")
            query = f"UPDATE {table} SET {','.join(field_values)} WHERE {condition}"
            cursor = self._connection.cursor()
            cursor.execute(query)
            cursor.close()
            self._connection.commit()
        except Exception as e:
            print(f"Update {table} failed, {str(e)}")
            return False

    def upsert(self, table: str, record: dict or List[dict], condition: str):
        """
        Upsert record in table
        """
        try:
            field_values = []
            for field, value in record.items():
                if isinstance(value, str):
                    value = f"'{value}'"
                field_values.append(f"{field}={value}")
            fields = ",".join(record.keys())
            values = tuple(record.values())
            query = f"""
                    DO $$
                        BEGIN
                            IF EXISTS(SELECT * FROM {table} WHERE {condition}) THEN
                               UPDATE {table} SET {','.join(field_values)} WHERE {condition};
                            ELSE
                               INSERT INTO {table} ({fields}) VALUES {values};
                            END IF;
                        END
                    $$
            """
            cursor = self._connection.cursor()
            cursor.execute(query)
            cursor.close()
            self._connection.commit()
            return True
        except Exception as e:
            print(f"Upsert {table} failed, {str(e)}")
            return False

    def is_exist(self, table: str, condition: str) -> bool:
        """
        Check record exist in mssql
        """
        try:
            query = f"SELECT * FROM {table} WHERE {condition}"
            cursor = self._connection.cursor()
            cursor.execute(query)
            record = cursor.fetchone()
            cursor.close()
            return record
        except Exception as e:
            print(f"Check is exist record failed, {str(e)}")

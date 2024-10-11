from dagster import resource, InitResourceContext
import pandas as pd
import os
from typing import List
import time
import sqlite3

class DbResource:
    def __init__(self, sqlite_database):
        self.conn = sqlite3.connect(sqlite_database)
        self.conn_id = sqlite_database

    def write_to_db(self, data: pd.DataFrame, table: str, if_exists: str, index: bool, retries=30, delay=5):
        """Write a DataFrame to a sqlitte db."""
        
        attempt = 0
        while attempt < retries:
            try:
                result = data.to_sql(table, self.conn, if_exists = if_exists, index = index)
                if result > 0:
                    print(f'{result} rows affected')
                    break
            
            except Exception as e:
                if "locked" in str(e):
                    attempt += 1
                    print(f"Database is locked. Retrying in {delay} seconds... (Attempt {attempt}/{retries})")
                    time.sleep(delay)
                    delay *= 2 if delay < 80 else 1 # Exponential backoff
                else:
                    raise Exception("Exception while writing to DB")
                
            finally:
              if attempt >= retries:
                print("Max retries reached. Could not write to the database.")
                raise Exception("Exception while writing to DB")
        
    def read_from_db(self, query: str) -> pd.DataFrame:
        """Read all DataFrames from Sqlite database"""
        data = pd.read_sql(query, self.conn)
        return data


@resource(config_schema={})
def sqlite_resource(init_context: InitResourceContext) -> DbResource:
    sqlite_database = os.getenv("SQLITE_DATABASE") 
    return DbResource(sqlite_database)

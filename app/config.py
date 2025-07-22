import os
import pyodbc

from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return pyodbc.connect(
        driver='{ODBC Driver 17 for SQL Server}',  # Use the correct driver
        server=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )
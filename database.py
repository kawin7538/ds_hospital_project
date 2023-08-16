import os
from dotenv import load_dotenv
import sqlalchemy

load_dotenv()

def get_conn():
    connection_url=sqlalchemy.engine.URL.create(drivername="mssql+pyodbc",username=os.getenv("DATABASE_USER"),password=os.getenv("DATABASE_PASSWORD"),host=os.getenv("DATABASE_HOST"),database=os.getenv("DATABASE_NAME"),query = {'driver':'ODBC Driver 17 for SQL Server','trusted_connection':'no'})
    
    engine=sqlalchemy.create_engine(connection_url)

    conn=engine.connect()

    return conn

if __name__ == '__main__':
    print(get_conn())
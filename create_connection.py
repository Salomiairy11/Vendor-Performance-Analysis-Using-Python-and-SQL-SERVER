from sqlalchemy import create_engine
import os
def create_connection():
    DRIVER = os.getenv("DRIVER")
    SERVER = os.getenv("SERVER")
    DATABASE = os.getenv("DATABASE")

    connection_url = (
        f"mssql+pyodbc://@{SERVER}/{DATABASE}"
        f"?driver={DRIVER}&trusted_connection=yes"
        )
    engine = create_engine(connection_url)
    return engine
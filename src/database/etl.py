# src/database/etl.py
import os
from dotenv import load_dotenv


from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import declarative_base


load_dotenv()

SQL_ALCHEMY_DATABASE_URI = os.getenv("SQL_ALCHEMY_DATABASE_URI")

print(SQL_ALCHEMY_DATABASE_URI)

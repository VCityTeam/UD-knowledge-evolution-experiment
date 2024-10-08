from sqlalchemy import create_engine, insert, text
import time
import os

# get hostname from environment variable
host = os.environ.get('HOSTNAME')
# get port from environment variable
port = os.environ.get('PORT')
# get database name from environment variable
db_name = os.environ.get('DBNAME')

engine = create_engine(f"postgresql+psycopg2://postgres:{host}@db:{port}/{db_name}")
conn = engine.connect()


query=text("<SQL QUERY>")
conn.execute(query)
conn.commit()

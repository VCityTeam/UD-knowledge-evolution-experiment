from psycopg2 import connect
import os
import time


# get product from environment variable
product = os.environ.get('PRODUCT')
# get version from environment variable
version = os.environ.get('VERSION')
# get step from environment variable
step = os.environ.get('STEP')

# get hostname from environment variable
host = os.environ.get('HOSTNAME')
# get port from environment variable
port = os.environ.get('PORT')
# get database name from environment variable
db_name = os.environ.get('DBNAME')
# get user from environment variable
user = os.environ.get('USER')
# get password from environment variable
password = os.environ.get('PASSWORD')

conn = connect(
    database=db_name, user=user, 
  password=password, host=host, port=port
)

conn.autocommit = True
cursor = conn.cursor()

query = f"SELECT pg_database_size('{db_name}');"

cursor.execute(query=query)
result = cursor.fetchone()

now = round(time.time())

print(f'{{"component":"{host}","space":"{result[0]}","version":"{version}","product":"{product}","step":"{step}","time":"{now}"}}')

conn.commit()
conn.close()
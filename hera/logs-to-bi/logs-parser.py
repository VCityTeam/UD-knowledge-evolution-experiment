import re
import os
import psycopg2
import sys

def extract_log_info(log_file_path: str):
    # Définir une expression régulière pour correspondre au format du log
    # {"component":"quaque-10-1-5-condensed-service","query":"./converg/converg-9.rq","try":"1","duration":"178ms","version":"1","product":"10","step":"15"}
    log_pattern = r'\{"component":"(?P<component>[^"]+)","query":"(?P<query>[^"]+)","try":"(?P<try>[^"]+)","duration":"(?P<duration>[^"]+)","version":"(?P<version>[^"]+)","product":"(?P<product>[^"]+)","step":"(?P<step>[^"]+)","time":"(?P<time>[^"]+)"\}'
    extracted_data = []

    # Lire le fichier de logs
    with open(log_file_path, 'r') as file:
        for line in file:
            # Chercher les correspondances avec le pattern
            match = re.search(log_pattern, line)
            if match:
                # Extraire COMPONENT, DURATION, et FILE
                component = match.group('component')
                # keep only the number of the query
                query = f"query-{match.group('query').split('-')[-1].split('.')[0]}"
                nb_try = int(match.group('try'))
                version_conf = int(match.group('version'))
                product_conf = int(match.group('product'))
                step_conf = int(match.group('step'))
                # Convertir le temps en millisecondes
                duration_ms = int(match.group('duration').replace("ms", ""))
                time_unix = int(match.group('time'))

                extracted_data.append({
                    "VERSION": version_conf,
                    "PRODUCT": product_conf,
                    "STEP": step_conf,
                    "COMPONENT": component,
                    "DURATION (ms)": duration_ms,
                    "QUERY": query,
                    "TRY": nb_try,
                    "TIME": time_unix
                })

    return extracted_data

def insert_logs_data(log_data_list, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT):
    """
    Connects to the PostgreSQL database and inserts a set of log records efficiently.

    Args:
        log_data_list (list[dict]): A list of dictionaries, each containing log data.
                                   Keys should match the expected JSON structure.
        DB_NAME (str): Database name.
        DB_USER (str): Database user.
        DB_PASSWORD (str): Database password.
        DB_HOST (str): Database host.
        DB_PORT (str): Database port.
    """
    import datetime
    
    conn = None  # Initialize conn to None
    cursor = None # Initialize cursor to None

    if not log_data_list:
        print("No log data provided to insert.")
        return # Nothing to do if the list is empty

    try:
        # Establish the database connection
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Database connection established successfully.")

        # Create a cursor object
        cursor = conn.cursor()

        # Define the SQL INSERT statement
        # Using parameterized queries (%(key)s) to prevent SQL injection
        insert_query = """
        INSERT INTO execution_logs (version, product, step, component, duration_ms, time, query, nb_try)
        VALUES (%(version)s, %(product)s, %(step)s, %(component)s, %(duration_ms)s, %(time)s, %(query)s, %(nb_try)s);
        """

        # Prepare the list of dictionaries for executemany
        # Each dictionary's keys must match the placeholders in the SQL query
        data_to_insert = []
        for log_entry in log_data_list:
             query_params = {
                 'version': log_entry.get("VERSION"),
                 'product': log_entry.get("PRODUCT"),
                 'step': log_entry.get("STEP"),
                 'component': log_entry.get("COMPONENT"),
                 'duration_ms': log_entry.get("DURATION (ms)"),
                 'time': datetime.datetime.fromtimestamp(log_entry.get("TIME")), 
                 'query': log_entry.get("QUERY"),
                 'nb_try': log_entry.get("TRY")
             }
             data_to_insert.append(query_params)

        if not data_to_insert:
            print("No valid log data formatted for insertion.")
            return # Exit if preparation resulted in an empty list

        print(f"Preparing to insert {len(data_to_insert)} log records.")

        # Execute the INSERT statement for all records using executemany
        # executemany is generally efficient for inserting multiple rows with the same statement structure
        cursor.executemany(insert_query, data_to_insert)
        print(f"{cursor.rowcount} records prepared for insertion (Note: rowcount might be -1 depending on driver/db).") # rowcount behavior can vary

        # Commit the transaction to make the changes permanent
        conn.commit()
        print("Transaction committed. Data inserted successfully.")

    except psycopg2.Error as e:
        # Handle potential database errors (connection, execution, etc.)
        print(f"Error connecting to or interacting with the database: {e}", file=sys.stderr)
        # Roll back the transaction if something went wrong mid-way
        if conn:
            try:
                conn.rollback()
                print("Transaction rolled back due to error.")
            except psycopg2.Error as rb_e:
                print(f"Error during rollback: {rb_e}", file=sys.stderr)
    except Exception as e:
        # Catch other potential errors during data preparation etc.
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        if conn: # Still try to rollback if connection exists
             try:
                conn.rollback()
                print("Transaction rolled back due to unexpected error.")
             except psycopg2.Error as rb_e:
                print(f"Error during rollback: {rb_e}", file=sys.stderr)

    finally:
        # Ensure the cursor and connection are closed even if errors occurred
        if cursor:
            try:
                cursor.close()
                print("Cursor closed.")
            except Exception as e:
                 print(f"Error closing cursor: {e}", file=sys.stderr)
        if conn:
            try:
                conn.close()
                print("Database connection closed.")
            except Exception as e:
                 print(f"Error closing connection: {e}", file=sys.stderr)



if __name__ == "__main__":
    # Afficher les informations extraites

    log_file_path = os.getenv("LOG_FILE_PATH")
    if not log_file_path:
        raise EnvironmentError(
            "LOG_FILE_PATH environment variable is not set.")
    
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")

    print(f"Extracting log data from {log_file_path}")
    log_data = extract_log_info(log_file_path)

    insert_logs_data(log_data, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)


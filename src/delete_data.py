import psycopg2
import calendar

def delete_data(year: int, month: int):
    """
    Connects to the PostgreSQL database and deletes a table with a name like 'data_2024-Oct.csv.gz'.
    """
    month_abbr = calendar.month_abbr[month]
    table_name = f"data_{year}-{month_abbr}.csv.gz"

    conn_params = {
        "dbname": "amazing_mspr_db",
        "user": "admin",
        "password": "admin",
        "host": "localhost",
        "port": 15432
    }

    try:
        print(f"\033[1;33mConnecting to PostgreSQL to delete table: {table_name}\033[0m")
        with psycopg2.connect(**conn_params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS "{table_name}";')
                print(f"\033[1;32mTable '{table_name}' deleted successfully (if it existed).\033[0m")
    except Exception as e:
        print(f"\033[1;31mError deleting table '{table_name}':\033[0m", e)

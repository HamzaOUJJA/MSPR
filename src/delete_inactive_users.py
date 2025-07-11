import psycopg2
import calendar
from datetime import datetime, timedelta

def delete_inactive_users():
    """
    Connects to the PostgreSQL database, identifies tables older than 12 months,
    and deletes users from those old tables who are no longer active (i.e.,
    do not appear in any of the latest 12 tables).
    """
    conn_params = {
        "dbname": "amazing_mspr_db",
        "user": "admin",
        "password": "admin",
        "host": "localhost",
        "port": 15432
    }

    try:
        print("\033[1;33mConnecting to PostgreSQL to process user data...\033[0m")
        with psycopg2.connect(**conn_params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # 1. Get all tables matching the naming pattern
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name LIKE 'data_____-___%.csv.gz';
                """)
                all_raw_tables = [row[0] for row in cur.fetchall()]

                relevant_tables = []
                for table_name in all_raw_tables:
                    try:
                        # Extract year and month abbreviation from table name
                        # e.g., 'data_2024-Oct.csv.gz' -> '2024-Oct'
                        date_str = table_name.split('_')[1].split('.')[0]
                        year_str, month_abbr_str = date_str.split('-')
                        year = int(year_str)
                        
                        # Convert month abbreviation to month number (1-12)
                        month_num = list(calendar.month_abbr).index(month_abbr_str)
                        
                        # Create a date object for comparison (first day of the month)
                        table_date = datetime(year, month_num, 1)
                        relevant_tables.append((table_name, table_date))
                    except (ValueError, IndexError):
                        print(f"\033[1;33mWarning: Skipping table '{table_name}' due to unexpected naming format.\033[0m")
                        continue

                if not relevant_tables:
                    print("\033[1;32mNo tables found matching the naming convention.\033[0m")
                    return

                # Sort tables by date to easily identify the latest 12
                relevant_tables.sort(key=lambda x: x[1], reverse=True)

                # Determine the cutoff date for "latest 12 months"
                # This needs to be carefully handled to correctly define "12 months old"
                # A simple approach is to get the current date and subtract 12 months.
                # We'll use the *start* of the current month, 12 months ago.
                today = datetime.now()
                # Calculate 12 months ago from the first day of the current month
                cutoff_date = (today.replace(day=1) - timedelta(days=365)).replace(day=1)

                latest_12_months_tables = []
                old_tables = []

                for table_name, table_date in relevant_tables:
                    if table_date >= cutoff_date:
                        latest_12_months_tables.append(table_name)
                    else:
                        old_tables.append(table_name)

                print(f"\033[1;36mFound {len(latest_12_months_tables)} tables from the last 12 months.\033[0m")
                print(f"\033[1;36mFound {len(old_tables)} tables older than 12 months.\033[0m")

                # 2. Collect active user IDs from the latest 12 tables
                active_user_ids = set()
                print("\033[1;33mCollecting active user IDs from the latest 12 tables...\033[0m")
                for table_name in latest_12_months_tables:
                    try:
                        cur.execute(f'SELECT DISTINCT user_id FROM "{table_name}";')
                        users_in_table = {row[0] for row in cur.fetchall()}
                        active_user_ids.update(users_in_table)
                    except psycopg2.Error as e:
                        print(f"\033[1;31mError querying table '{table_name}':\033[0m", e)
                        # Decide how to handle this: skip table, raise error, etc.
                        # For now, we'll just log and continue.

                print(f"\033[1;32mTotal active users identified: {len(active_user_ids)}\033[0m")

                # 3. Process old tables to delete inactive users
                if not old_tables:
                    print("\033[1;32mNo old tables to process for inactive users.\033[0m")
                    return

                print("\033[1;33mProcessing old tables for inactive users...\033[0m")
                for table_name in old_tables:
                    try:
                        cur.execute(f'SELECT user_id FROM "{table_name}";')
                        users_in_old_table = {row[0] for row in cur.fetchall()}

                        inactive_users_to_delete = users_in_old_table - active_user_ids

                        if inactive_users_to_delete:
                            # Convert set to list/tuple for SQL IN clause
                            inactive_users_list = tuple(inactive_users_to_delete)
                            # Ensure proper formatting for the IN clause with varying number of items
                            # If only one item, (item,) is needed
                            placeholders = ', '.join(['%s'] * len(inactive_users_list))
                            
                            delete_sql = f'DELETE FROM "{table_name}" WHERE user_id IN ({placeholders});'
                            cur.execute(delete_sql, inactive_users_list)
                            print(f"\033[1;32mDeleted {len(inactive_users_to_delete)} inactive users from table '{table_name}'.\033[0m")
                        else:
                            print(f"\033[1;34mNo inactive users found in table '{table_name}'.\033[0m")

                        # Optional: If the table becomes empty after deletions, consider dropping it
                        # cur.execute(f'SELECT COUNT(*) FROM "{table_name}";')
                        # if cur.fetchone()[0] == 0:
                        #     cur.execute(f'DROP TABLE "{table_name}";')
                        #     print(f"\033[1;32mTable '{table_name}' is now empty and has been dropped.\033[0m")

                    except psycopg2.Error as e:
                        print(f"\033[1;31mError processing table '{table_name}':\033[0m", e)

            print("\033[1;32mInactive user deletion process completed.\033[0m")

    except Exception as e:
        print(f"\033[1;31mAn error occurred during database operation:\033[0m", e)


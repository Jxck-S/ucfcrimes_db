import psycopg2
from configparser import ConfigParser
main_config = ConfigParser()
main_config.read('config.ini')
import time, calendar, datetime
table = main_config.get('postgresql', 'table')

def setup_db(main_config):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host = main_config.get('postgresql', 'host'),
        database = main_config.get('postgresql', 'database'),
        user = main_config.get('postgresql', 'user'),
        password = main_config.get('postgresql', 'password')

    )

    cur = conn.cursor()
    return conn, cur

def insert_case(cur, table, d):
    # Define the SQL statement to insert data into the table
    sql = f"""
    INSERT INTO {table} (disposition, case_id, report_dt, crime, start_dt, location, end_dt, campus)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Loop through the list of dictionaries and execute the SQL statement for each one
    cur.execute(sql, (d["disposition"], d["case_id"], d["report_dt"], d["crime"], d["start_dt"], d["location"], d["end_dt"], d["campus"]))

conn, cur = setup_db(main_config)
from get_crimes import crime_load
sql = f"""
SELECT case_id
FROM {table}
ORDER BY id DESC
LIMIT 1
"""
# Execute the SQL statement with fetchone() method
cur.execute(sql)
if cur.rowcount > 0:
    latest_case_id = cur.fetchone()[0]
    print(f"Latest case is {latest_case_id}")
else:
    latest_case_id = None

while True:
    if not conn:
        conn, cur = setup_db(main_config)

    # Define the list of dictionaries
    crimes = crime_load()

    if not latest_case_id:
        latest_case_id = crimes['cases'][-1]['case_id']
        print(f"Started newest case is  {latest_case_id}")

    elif latest_case_id != crimes['cases'][-1]['case_id']:
        new_count = 0
        #Find highest index of crime in the cases, in case of the same case occuring twice due to update.
        for idx, case in enumerate(crimes['cases']):
            if case['case_id'] == latest_case_id:
                highest_idx_latest = idx

        #iterate only new
        for i in range(highest_idx_latest+1, len(crimes['cases'])):
            new_count += 1
            case = crimes['cases'][i]
            insert_case(cur, table, case)
            print(f"New case {case['case_id']}")
        latest_case_id = crimes['cases'][-1]['case_id']
        print(f"New cases detected  {new_count}")
    # Commit the changes and close the cursor and connection
    conn.commit()
    cur.close()
    conn.close()
    conn = None
    cur = None
    import os
    os.remove("AllDailyCrimeLog.pdf")
    # get current date and time
    now = datetime.datetime.now()
    # get the number of days in the current month
    last_day = calendar.monthrange(now.year, now.month)[1]
    # calculate time until next day at 12:05 am
    if now.day == last_day:
        target_time = datetime.datetime(now.year, now.month+1, 1, 0, 3)
    else:
        target_time = datetime.datetime(now.year, now.month, now.day+1, 0, 3)
    time_delta = target_time - now
    print(f"Sleeping until {target_time} ({time_delta})")
    seconds_until_target = time_delta.total_seconds()
    time.sleep(seconds_until_target)
    print("Running new check")


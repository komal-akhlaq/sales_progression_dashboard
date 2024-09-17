import os
import psycopg2
import csv
import pandas as pd
import json
import streamlit as st
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors


from datetime import datetime, timedelta

# List of employee names
employee_names = ['Mukund Chopra','John Green', 'Sara Edward','Ryan Rehman','Omar Blake','Simon Sinek','Daniel Robinson', 'Moohi Ahmed','Waseem Zubair','Alina Victor']

# defining the time
# Calculate the time range for the past 24 hours
end_time = datetime.now()

# Set the start time to 7 PM yesterday
start_time = end_time - timedelta(days=1)
start_time = start_time.replace(hour=13, minute=0, second=0)

# end time is 12 hours after start time
end_time = start_time + timedelta(hours=12)

# Convert times to string format for SQL
start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

print(f"Start time: {start_time_str}")
print(f"End time: {end_time_str}")

"""# Reading from database"""

# reading clients.csv
# Database connection parameters
db_params = {
        'dbname': st.secrets["database"]["DB_NAME"],
        'user': st.secrets["database"]["DB_USER"],
        'password': st.secrets["database"]["DB_PASSWORD"],
        'host': st.secrets["database"]["DB_HOST"],
        'port': st.secrets["database"]["DB_PORT"]
    }

# SQL query template to fetch distinct client IDs and names for the past month
fetch_client_ids_query = """
SELECT DISTINCT c.id AS client_id, c.fullname AS client_name
FROM
(
    SELECT
        t.client_id
    FROM
        textmessage t
    JOIN
        employee e ON t.created_by = e.id
    WHERE
        t.created >= NOW() - INTERVAL '1 month'
    AND e.fullname = ANY(%s)

    UNION

    SELECT
        ol.client_id
    FROM
        openphone_log ol
    JOIN
        employee e ON ol.from_ = e.phone
    WHERE
        ol.created_at_parsed >= NOW() - INTERVAL '1 month'
    AND ol.direction = 'outgoing'
    AND e.fullname = ANY(%s)
) as combined
JOIN
    public.client c ON combined.client_id = c.id
ORDER BY client_id;
"""

def fetch_client_ids_and_names():
    connection = None
    cursor = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Execute the query to fetch distinct client IDs and names for the past month
        cursor.execute(fetch_client_ids_query, (employee_names, employee_names))

        # Fetch all client IDs and names
        client_data = cursor.fetchall()

        # Write the client IDs and names to a CSV file
        with open('clients.csv', mode='w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['client_id', 'client_name'])  # Write header

            for row in client_data:
                csv_writer.writerow(row)

        print("Client IDs and names have been saved to clients.csv")

    except Exception as error:
        print(f"Error fetching client data: {error}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Run the function to fetch client IDs and names and save to CSV
fetch_client_ids_and_names()

# SQL query template to fetch the required records for the specified time range
fetch_records_query_template = f"""
(
    SELECT
        to_char(t.created, 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
        'text_created' AS type,
        t.message AS message,
        t.client_id,
        e.fullname AS employee_name
    FROM
        textmessage t
    JOIN
        employee e ON t.created_by = e.id
    WHERE
        e.fullname = %s
        AND t.created BETWEEN '{start_time_str}' AND '{end_time_str}'
)
UNION ALL
(
    SELECT
        to_char(ol.created_at_parsed, 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
        'call_created' AS type,
        NULL AS message,
        ol.client_id,
        e.fullname AS employee_name
    FROM
        openphone_log ol
    JOIN
        employee e ON ol.from_ = e.phone
    WHERE
        e.fullname = %s
        AND ol.created_at_parsed BETWEEN '{start_time_str}' AND '{end_time_str}'
        AND ol.direction = 'outgoing'
)
UNION ALL
(
    SELECT
        to_char(ol.completed_at_parsed, 'YYYY-MM-DD HH24:MI:SS') AS timestamp,
        'call_completed' AS type,
        NULL AS message,
        ol.client_id,
        e.fullname AS employee_name
    FROM
        openphone_log ol
    JOIN
        employee e ON ol.from_ = e.phone
    WHERE
        e.fullname = %s
        AND ol.completed_at_parsed BETWEEN '{start_time_str}' AND '{end_time_str}'
        AND ol.direction = 'outgoing'
)
ORDER BY
    client_id, timestamp;
"""

def fetch_and_save_records_to_csv():
    connection = None
    cursor = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Open a CSV file to write the results
        with open('employee_records.csv', mode='w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['timestamp', 'type', 'message', 'client id', 'employee_name'])

            # Loop over each employee name and fetch records
            for name in employee_names:
                cursor.execute(fetch_records_query_template, (name, name, name))

                # Fetch all records
                records = cursor.fetchall()

                # Write each record to the CSV file
                for record in records:
                    csv_writer.writerow(record)

                # Print the number of records fetched for the current employee
                # print(f"Number of records fetched for {name}: {len(records)}")
            print("Employee records fetched")

    except Exception as error:
        print(f"Error fetching records: {error}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Run the function to fetch records and save to CSV
fetch_and_save_records_to_csv()

"""# Employee Progress (New)"""

def run_query_and_save_to_csv(sql_query, csv_file_name):
    connection = None
    cursor = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(sql_query)

        # Fetch all records
        records = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Open a CSV file to write the results
        with open(csv_file_name, mode='w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)

            # Write the column headers
            csv_writer.writerow(column_names)

            # Write the records
            csv_writer.writerows(records)

        print(f"Records saved to {csv_file_name}")

    except Exception as error:
        print(f"Error running query: {error}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

sql_query = f"""
SELECT
    csp.id,
    csp.client_id,
    c.fullname,
    CASE
        WHEN csp.current_stage = 1 THEN 'Stage 1: Not Interested'
        WHEN csp.current_stage = 2 THEN 'Stage 2: Initial Contact'
        WHEN csp.current_stage = 3 THEN 'Stage 3: Requirement Collection'
        WHEN csp.current_stage = 4 THEN 'Stage 4: Property Touring'
        WHEN csp.current_stage = 5 THEN 'Stage 5: Property Tour and Feedback'
        WHEN csp.current_stage = 6 THEN 'Stage 6: Application and Approval'
        WHEN csp.current_stage = 7 THEN 'Stage 7: Post-Approval and Follow-Up'
        WHEN csp.current_stage = 8 THEN 'Stage 8: Commission Collection'
        WHEN csp.current_stage = 9 THEN 'Stage 9: Dead Stage'
        ELSE 'Unknown Stage'
    END AS stage_name,
    csp.current_stage,
    csp.created_on,
    c.assigned_employee,
    c.assigned_employee_name
FROM
    client_stage_progression csp
JOIN
    client c
ON
    csp.client_id = c.id
WHERE
    csp.created_on BETWEEN '{start_time_str}' AND '{end_time_str}'
ORDER BY
    csp.created_on;

"""

csv_file_name = 'progress.csv'

# Run the function to execute the query and save to CSV
run_query_and_save_to_csv(sql_query, csv_file_name)

df5 = pd.read_csv('progress.csv')

# drop duplicates based on client_id, stage_name and assigned_employee
df5 = df5.drop_duplicates(subset=['client_id', 'current_stage'])

# drop all rows where current_stage is 9
df5 = df5[df5['current_stage'] != 9]

def employee_record(name, df):
    temp = df[df['assigned_employee_name'] == name]

    # group the dataframe by 'assigned_employee_name' and calculate the minimum and maximum 'current_stage' and 'stage_name'
    result_df = temp.groupby('fullname')[['current_stage', 'stage_name']].agg(['min', 'max']).reset_index()

    # rename the columns
    result_df.columns = ['Client', 'min_current_stage', 'max_current_stage', 'min_stage_name', 'max_stage_name']

    # create a new column change, which is the difference between the maximum and minimum current_stage
    result_df['change'] = result_df['max_current_stage'] - result_df['min_current_stage']

    # new colum Previous stage such that f'{min_current_stage}: {min_stage_name.split(':')[1]}' and check if : is present in min_stage_name to split
    result_df['Previous Stage'] = result_df['min_current_stage'].astype(str) + ': ' + result_df['min_stage_name'].apply(lambda x: x.split(':')[1] if ':' in x else x)

    # Current stage such that f'{max_current_stage}: {max_stage_name.split(':')[1]}' and check if : is present in max_stage_name to split
    result_df['Current Stage'] = result_df['max_current_stage'].astype(str) + ': ' + result_df['max_stage_name'].apply(lambda x: x.split(':')[1] if ':' in x else x)

    # check if - in the stage name, split the string and get the first element
    result_df['Previous Stage'] = result_df['Previous Stage'].apply(lambda x: x.split(' - ')[0])

    # check if - in the stage name, split the string and get the first element
    result_df['Current Stage'] = result_df['Current Stage'].apply(lambda x: x.split(' - ')[0])

    # drop the columns min_current_stage, max_current_stage, min_stage_name and max_stage_name, change
    result_df = result_df.drop(columns=['min_current_stage', 'max_current_stage', 'min_stage_name', 'max_stage_name','change'])
    return result_df

"""# Creating appropriate data"""

client_ids = {}
# reading client names
df = pd.read_csv(f'clients.csv')

# creating a dictionary with client ids and names
for index, row in df.iterrows():
    client_ids[row['client_id']] = row['client_name']

df = pd.read_csv(f'employee_records.csv')

# dropping the message column as it is of no use
df.drop('message', axis=1, inplace=True)

# creting a timestamp column
df['time_stamp'] = pd.to_datetime(df['timestamp'])

# Iterate through the rows to calculate the duration
for i in range(len(df) - 1):
    if df.loc[i, 'type'] == 'call_created' and df.loc[i+1, 'type'] == 'call_completed':
        # Convert timestamps to Unix time for calculation
        start_unix = df.loc[i, 'time_stamp'].timestamp()
        end_unix = df.loc[i+1, 'time_stamp'].timestamp()

        # Calculate the duration in seconds
        duration = end_unix - start_unix
        df.loc[i, 'call_duration'] = duration

# create a column for client name
df['client_name'] = df['client id'].map(client_ids)

# Drop the rows with type 'call_completed' as they are not needed
df = df[df['type'] != 'call_completed']

# change rows where type is call_created and call duration is null to 0
df.loc[(df['type'] == 'call_created') & (df['call_duration'].isnull()), 'call_duration'] = 0

# add average of call duration of that employee where call duration is 0
for index, row in df.iterrows():
    if row['call_duration'] == 0:
        df.loc[index, 'call_duration'] = df[df['employee_name'] == row['employee_name']]['call_duration'].mean()

# drop the time_stamp column
df.drop('time_stamp', axis=1, inplace=True)

"""## Report generation (New)"""

# Constants
ASSIGNED_MINUTES = 480  # 8 hours in minutes
SECONDS_PER_MESSAGE = 5  # 5 seconds per message

def add_employee_report(employee_name, df, df5, elements):
    styles = getSampleStyleSheet()

    # Add employee report title
    elements.append(Paragraph(f'Report for {employee_name}', styles['Heading2']))
    elements.append(Spacer(1, 12))

    # Collect one-line answers
    total_calls = df[df['employee_name'] == employee_name]['call_duration'].count()
    if total_calls > 0:
        elements.append(Paragraph(f'Total Calls: {total_calls}', styles['Normal']))

    total_duration_seconds = df[df['employee_name'] == employee_name]['call_duration'].sum()
    total_duration_minutes = total_duration_seconds // 60
    total_duration_remaining_seconds = total_duration_seconds % 60

    if total_duration_minutes > 0 or total_duration_remaining_seconds > 0:
        elements.append(Paragraph(f'Total Call Duration: {int(total_duration_minutes)} minutes {int(total_duration_remaining_seconds)} seconds', styles['Normal']))

    total_messages = df[(df['employee_name'] == employee_name) & (df['type'] == 'text_created')].shape[0]
    if total_messages > 0:
        elements.append(Paragraph(f'Total Messages: {total_messages}', styles['Normal']))

    # Calculate total message time in seconds
    total_message_time_seconds = total_messages * SECONDS_PER_MESSAGE

    # Calculate total work time in seconds (call duration + message time)
    total_work_time_seconds = total_duration_seconds + total_message_time_seconds

    # Convert total work time into minutes and seconds
    total_work_time_minutes = total_work_time_seconds // 60
    total_work_time_remaining_seconds = total_work_time_seconds % 60

    # Add the number of minutes assigned (8 hours = 480 minutes)
    elements.append(Paragraph(f'Assigned Time: {ASSIGNED_MINUTES} minutes', styles['Normal']))
    elements.append(Paragraph(f'Total Work Time: {int(total_work_time_minutes)} minutes {int(total_work_time_remaining_seconds)} seconds', styles['Normal']))

    # Determine number of clients handled
    employee_calls_clients = df[(df['employee_name'] == employee_name) & (df['type'] == 'call_created')]['client_name'].dropna().unique()
    employee_messages_clients = df[(df['employee_name'] == employee_name) & (df['type'] == 'text_created')]['client_name'].dropna().unique()

    # Merge both client lists and get unique clients
    unique_clients = pd.Series(list(set(employee_calls_clients) | set(employee_messages_clients))).dropna().unique()
    num_clients = len(unique_clients)

    if num_clients > 0:
        elements.append(Paragraph(f'Number of Clients Handled: {num_clients}', styles['Normal']))

    elements.append(Spacer(1, 12))

    # Add employee-specific record table
    employee_df = employee_record(employee_name, df5)
    if not employee_df.empty:
        elements.append(Paragraph("Employee Records:", styles['Heading2']))

        # Convert the employee records DataFrame to a list of lists for the Table
        record_data = [employee_df.columns.tolist()] + employee_df.values.tolist()

        # Create the table with styles
        record_table = Table(record_data)
        record_table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Bold for header row
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),  # White text for header
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),  # Grey background for header
        ]))
        elements.append(record_table)
        elements.append(Spacer(1, 12))

    # Add separator line
    elements.append(Paragraph('---------------------------------------------------------------------------------------------------------------------------------------', styles['Normal']))
    elements.append(Spacer(1, 12))

def generate_combined_pdf_report(df, df5):
    pdf_filename = 'combined_employee_report.pdf'
    doc = SimpleDocTemplate(pdf_filename, pagesize=letter)
    elements = []

    employee_names = df['employee_name'].unique()
    for employee_name in employee_names:
        add_employee_report(employee_name, df, df5, elements)

    doc.build(elements)

# Assuming df, df5, and progress are defined
generate_combined_pdf_report(df, df5)

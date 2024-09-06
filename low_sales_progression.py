import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime

# def messageParser(client_id: int):
    # db_params = {
    #     'dbname': st.secrets["database"]["DB_NAME"],
    #     'user': st.secrets["database"]["DB_USER"],
    #     'password': st.secrets["database"]["DB_PASSWORD"],
    #     'host': st.secrets["database"]["DB_HOST"],
    #     'port': st.secrets["database"]["DB_PORT"]
    # }
#     try:
#         connection = psycopg2.connect(**db_params)
#         cursor = connection.cursor()
#         query = '''
#             SELECT client.fullname, employee.fullname, message, status, textmessage.created
#             FROM textmessage 
#             JOIN client ON client.id = client_id
#             JOIN employee ON employee.id = employee_id
#             WHERE client_id = %s
#             ORDER BY textmessage.created ASC
#         '''
#         cursor.execute(query, (client_id,))
#         messages_raw = cur.fetchall()

#         messages = [
#             f"[{msg[-1]}] {'Client' if msg[3] == 'Received' else 'Sales Rep'}: {msg[2]}"
#             for msg in messages_raw
#         ]

#         cursor.close()
#         connection.close()

#         return '\n'.join(messages)
#     except Exception as e:
#         print(e)
#         return None

def show_low_sales_progression():
    st.title("Low Sales Progression Report")
    
    db_params = {
        'dbname': st.secrets["database"]["DB_NAME"],
        'user': st.secrets["database"]["DB_USER"],
        'password': st.secrets["database"]["DB_PASSWORD"],
        'host': st.secrets["database"]["DB_HOST"],
        'port': st.secrets["database"]["DB_PORT"]
    }
    # Employee IDs to filter
    employee_ids = [378, 375, 356, 373, 333, 173]

    fetch_low_progression_clients_query = f"""
    SELECT 
        csp.client_id,
        c.fullname AS client_name,
        e.fullname AS employee_name,
        MAX(csp.current_stage) AS current_stage,
        MAX(csp.created_on) AS time_entered_stage,
        CONCAT('https://services.followupboss.com/2/people/view/', csp.client_id) AS followup_boss_link
    FROM 
        public.client_stage_progression csp
    JOIN 
        public.client c ON csp.client_id = c.id
    JOIN 
        public.employee e ON c.assigned_employee = e.id
    WHERE 
        csp.current_stage <= 3
        AND csp.created_on >= NOW() - INTERVAL '24 hours'
        AND e.id IN ({','.join(map(str, employee_ids))})
    GROUP BY 
        csp.client_id, c.fullname, e.fullname
    HAVING 
        MAX(csp.current_stage) <= 3
    ORDER BY 
        e.fullname, csp.client_id;
    """

    def fetch_data(query):
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**db_params)
            cursor = connection.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(records, columns=column_names)
            
            return df
            
        except Exception as error:
            st.error(f"Error fetching records: {error}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def display_low_progression_clients(df):
        st.subheader("Clients with Low Progression in the Last 24 Hours")
        if df.empty:
            st.write("No clients found with low progression in the last 24 hours.")
            return

        for idx, row in df.iterrows():
            st.write(f"**Sales Rep:** {row['employee_name']}")
            st.write(f"**Client:** {row['client_name']} - [FUB Link]({row['followup_boss_link']})")
            st.write(f"**Current Stage:** {row['current_stage']}")
            st.write("---")

            # Commented out the message display part
            # messages = messageParser(row['client_id'])
            # st.text_area(
            #     f"Messages with {row['client_name']} (Client ID: {row['client_id']})", 
            #     messages if messages else "No messages found.", 
            #     height=150,
            #     key=f"messages_{row['client_id']}_{idx}"  # Unique key based on client_id and index
            # )

    # The "Show Data / Refresh Data" button is not needed since the page refreshes automatically
    today = datetime.today().strftime('%Y-%m-%d')
    st.markdown(f"**DATE: {today}** (This report contains data from the last 24 hours)")

    low_progression_clients_data = fetch_data(fetch_low_progression_clients_query)

    if low_progression_clients_data is not None:
        display_low_progression_clients(low_progression_clients_data)

import streamlit as st
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

def show_client_stage_progression():
    st.title("Client Stage Progression Report")

    db_params = {
        'dbname': st.secrets["database"]["DB_NAME"],
        'user': st.secrets["database"]["DB_USER"],
        'password': st.secrets["database"]["DB_PASSWORD"],
        'host': st.secrets["database"]["DB_HOST"],
        'port': st.secrets["database"]["DB_PORT"]
    }
    
    fetch_leads_stage_4_and_beyond_query = """
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
            csp.current_stage >= 4
            AND csp.created_on >= NOW() - INTERVAL '24 hours'
        GROUP BY 
            csp.client_id, c.fullname, e.fullname
        ORDER BY 
            csp.client_id;
    """

    fetch_sales_reps_count_query = """
    WITH latest_stage_progression AS (
    SELECT 
        csp.client_id,
        e.fullname AS employee_name,
        DATE(MAX(csp.created_on)) AS date_moved
    FROM 
        public.client_stage_progression csp
    JOIN 
        public.client c ON csp.client_id = c.id
    JOIN 
        public.employee e ON c.assigned_employee = e.id
    WHERE 
        csp.current_stage >= 4
        AND csp.created_on >= NOW() - INTERVAL '24 hours'
    GROUP BY 
        csp.client_id, e.fullname
    )
    SELECT 
        employee_name,
        date_moved,
        COUNT(client_id) AS count_of_leads
    FROM 
        latest_stage_progression
    GROUP BY 
        employee_name, date_moved
    ORDER BY 
        date_moved DESC, count_of_leads DESC;
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

    def plot_leads_stage_4_and_beyond(df):
        st.subheader("Bar Chart of Clients in Property Touring and Beyond")
        fig, ax = plt.subplots(figsize=(14, 8)) 

        # Check if the DataFrame is empty
        if df.empty:
            st.write("No data available for the selected period.")
            return

        # Map stage numbers to names
        stage_mapping = {
            4: 'Stage 4: Property Touring',
            5: 'Stage 5: Property Tour and Feedback',
            6: 'Stage 6: Application and Approval',
            7: 'Stage 7: Post-Approval and Follow-Up',
            8: 'Stage 8: Commission Collection'
        }
        
        df['stage_name'] = df['current_stage'].map(stage_mapping)

        # Group by stage_name and count the number of clients in each stage
        stage_counts = df['stage_name'].value_counts().sort_index()

        # Plot the bar chart
        stage_counts.plot(kind='bar', ax=ax)
        ax.set_xlabel('Stage', fontsize=12)
        ax.set_ylabel('Number of Clients', fontsize=12)
        ax.set_title('Clients in Property Touring and Beyond', fontsize=16)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right', fontsize=10)
        st.pyplot(fig)
    
    def plot_sales_reps_moving_leads(df):
        st.subheader("Graph: Sales Reps Moving Leads to Property Touring and Beyond")
        fig, ax = plt.subplots(figsize=(14, 8)) 
        pivot_data = df.pivot(index='date_moved', columns='employee_name', values='count_of_leads').fillna(0)
        pivot_data.plot(kind='bar', stacked=True, ax=ax)
        ax.set_xlabel('Date', fontsize=12)
        ax.set_ylabel('Number of Leads', fontsize=12)
        ax.set_title('Sales Reps Moving Leads to Property Touring and Beyond', fontsize=16)
        plt.xticks(rotation=45, ha='right', fontsize=10)
        plt.yticks(fontsize=10)
        ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5), fontsize=10) 
        st.pyplot(fig)
    
    def create_employee_stage_table(df):
        st.subheader("Number of Clients in Each Stage per Employee")
        pivot_df = df.pivot_table(index='employee_name', columns='current_stage', aggfunc='size', fill_value=0)
        pivot_df = pivot_df.rename(columns={4: 'Stage 4: Property Touring', 5: 'Stage 5: Property Tour and Feedback', 6: 'Stage 6: Application and Approval', 7: 'Stage 7: Post-Approval and Follow-Up', 8: 'Stage 8: Commission Collection'})
        st.dataframe(pivot_df)

    # The "Show Data / Refresh Data" button is not needed since the page refreshes automatically
    today = datetime.today().strftime('%Y-%m-%d')
    st.markdown(f"**DATE: {today}** (This report contains data from the last 24 hours)")

    leads_data = fetch_data(fetch_leads_stage_4_and_beyond_query)

    if leads_data is not None:
        st.subheader("Leads in Property Touring and Beyond")
        st.dataframe(leads_data)
        st.write(f"Total leads in Property Touring and beyond: {len(leads_data)}")
        
        plot_leads_stage_4_and_beyond(leads_data)
        create_employee_stage_table(leads_data)

    sales_reps_data = fetch_data(fetch_sales_reps_count_query)

    if sales_reps_data is not None:
        st.subheader("Sales Reps Moving Leads to Property Touring and Beyond")
        st.dataframe(sales_reps_data)
        st.write(f"Total entries: {len(sales_reps_data)}")
        plot_sales_reps_moving_leads(sales_reps_data)
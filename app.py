import streamlit as st
from sales_leads import show_sales_leads
from client_stage_progression import show_client_stage_progression
from low_sales_progression import show_low_sales_progression
from streamlit_autorefresh import st_autorefresh

favicon = "2.png"
st.set_page_config(page_title='Homeeasy Sales Dashboard', page_icon=favicon, layout='wide', initial_sidebar_state='auto')

if "refresh_count" not in st.session_state:
    st.session_state.refresh_count = 0
st.session_state.refresh_count += 1
# st.sidebar.write(f"Page refreshed {st.session_state.refresh_count} times.")
st.sidebar.write("The page will refresh automatically every hour.")

# Set the refresh interval to 1 hour 
st_autorefresh(interval=3600 * 1000, key="autoRefresh", debounce=False)
st.sidebar.title("Homeeasy Sales Leads Monitoring System")
page = st.sidebar.selectbox("Choose a report", ["Sales Leads Monitoring", "Client Stage Progression Report", "Low Sales Progression"])
if page == "Sales Leads Monitoring":
    show_sales_leads()
elif page == "Client Stage Progression Report":
    show_client_stage_progression()
else:
    show_low_sales_progression()

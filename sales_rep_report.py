import streamlit as st
import fitz  # PyMuPDF
from PIL import Image
import io
import os

def show_sales_rep_daily_report():
    # Optionally run an external script (make sure the script exists and works)
    os.system('python automatic_report.py')
    
    st.title("Daily Report")

    # Ensure the PDF file exists before proceeding
    pdf_file = "combined_employee_report (1).pdf"

    if os.path.exists(pdf_file):
        # Open the PDF using PyMuPDF
        pdf_document = fitz.open(pdf_file)
        
        # Iterate through each page
        for page_num in range(pdf_document.page_count):
            page = pdf_document.load_page(page_num)  # Get a specific page
            pix = page.get_pixmap()  # Render page to an image
            
            # Convert pixmap to a PNG image
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            
            # Display the image of the PDF page in Streamlit
            st.image(img, caption=f"Page {page_num + 1}", use_column_width=True)
        
        pdf_document.close()
    else:
        st.error(f"File not found: {pdf_file}")

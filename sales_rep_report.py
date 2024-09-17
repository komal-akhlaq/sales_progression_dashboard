import streamlit as st
import fitz  # PyMuPDF
from PIL import Image
import io
import os

def show_sales_rep_daily_report():
    os.system('python automatic_report.py')
    st.title("Daily Report")

    # Load the local PDF file (replace 'sample.pdf' with your PDF path)
    pdf_file = "combined_employee_report (1).pdf"

    # Open the PDF using PyMuPDF
    pdf_document = fitz.open(pdf_file)
    
    # Iterate through each page
    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)  # Get a specific page
        pix = page.get_pixmap()  # Render page to an image
        img = Image.open(io.BytesIO(pix.tobytes("png")))  # Convert to PIL image
        
        # Display the image of the PDF page in Streamlit
        st.image(img, caption=f"Page {page_num + 1}", use_column_width=True)
    
    pdf_document.close()

import streamlit as st
import os
import tempfile
import time
from main import process_annual_report
import pandas as pd
from pathlib import Path

# Configure Streamlit page
st.set_page_config(
    page_title="Annual Report Extractor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .upload-section {
        background-color: #f0f2f6;
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .success-message {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
    }
    .error-message {
        background-color: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #f5c6cb;
    }
    .info-box {
        background-color: #d1ecf1;
        color: #0c5460;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #bee5eb;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Header
    st.markdown('<h1 class="main-header">📊 Annual Report Financial Statement Extractor</h1>', unsafe_allow_html=True)
    
    # Sidebar with information
    with st.sidebar:
        st.header("ℹ️ About")
        st.markdown("""
        This tool extracts financial statements from annual report PDFs and generates Excel files with:
        
        **Financial Statements:**
        - Statement of Profit or Loss
        - Statement of Comprehensive Income  
        - Statement of Financial Position
        - Statement of Changes in Equity
        - Statement of Cash Flows
        
        **SOP Metrics (Key/Value):**
        - Metrics like Revenue, Gross Profit, Net Profit, Assets, etc.
        - Calculated ratios and metrics
        - Output format: two columns — `Metric`, `Value`
        """)
        
        st.header("📋 Instructions")
        st.markdown("""
        1. Upload your annual report PDF
        2. Click 'Process PDF' to extract data
        3. Download the generated Excel file
        4. Check the 'SOP_Metrics' sheet for calculated metrics
        """)
        
        # Show available Excel files with download buttons
        st.header("📁 Available Files")
        excel_files = get_available_excel_files()
        if excel_files:
            # Add option to show all files
            show_all = st.checkbox("Show all files", value=False, help="Show all generated Excel files")
            
            # Determine how many files to show
            files_to_show = excel_files if show_all else excel_files[:5]
            
            # Create a scrollable container
            with st.container():
                for file in files_to_show:
                    file_path = os.path.join("Excel_Statements", file)
                    if os.path.exists(file_path):
                        # File info
                        st.text(f"📄 {file}")
                        
                        # Download button
                        with open(file_path, "rb") as f:
                            st.download_button(
                                label="📥 Download",
                                data=f.read(),
                                file_name=file,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"sidebar_download_{file}",
                                help=f"Download {file}",
                                use_container_width=True
                            )
                        
                        st.divider()  # Add separator between files
            
            if not show_all and len(excel_files) > 5:
                st.caption(f"Showing 5 of {len(excel_files)} files. Check 'Show all files' to see all.")
        else:
            st.text("No Excel files found")

    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<div class="upload-section">', unsafe_allow_html=True)
        st.header("📤 Upload Annual Report PDF")
        
        # File upload widget
        uploaded_file = st.file_uploader(
            "Choose a PDF file",
            type=['pdf'],
            help="Upload your annual report PDF file for processing"
        )
        
        # Process button
        if uploaded_file is not None:
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            
            if st.button("🔄 Process PDF", type="primary", use_container_width=True):
                process_pdf_file(uploaded_file)
        else:
            st.info("👆 Please upload a PDF file to begin processing")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Add section to preview all financial statements
    st.header("📋 Financial Statements Preview")
    
    # Show available Excel files for preview
    excel_files = get_available_excel_files()
    if excel_files:
        # Create tabs for each Excel file
        if len(excel_files) > 0:
            selected_file = st.selectbox(
                "Select an Excel file to preview:",
                excel_files,
                key="file_selector"
            )
            
            if selected_file:
                file_path = os.path.join("Excel_Statements", selected_file)
                show_all_statements_preview(file_path)
    else:
        st.info("No Excel files available for preview. Upload and process a PDF first.")
    
    with col2:
        st.header("📁 Available Files")
        
        # Show available Excel files with download buttons
        excel_files = get_available_excel_files()
        if excel_files:
            # Add option to show all files
            show_all_main = st.checkbox("Show all files", value=False, help="Show all generated Excel files", key="main_show_all")
            
            # Determine how many files to show
            files_to_show = excel_files if show_all_main else excel_files[:5]
            
            # Create a scrollable container
            with st.container():
                for file in files_to_show:
                    file_path = os.path.join("Excel_Statements", file)
                    if os.path.exists(file_path):
                        # Create columns for file info and download button
                        col_file, col_btn = st.columns([3, 1])
                        
                        with col_file:
                            st.text(f"📄 {file}")
                        
                        with col_btn:
                            # Download button for this file
                            with open(file_path, "rb") as f:
                                st.download_button(
                                    label="📥",
                                    data=f.read(),
                                    file_name=file,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"download_{file}",
                                    help=f"Download {file}"
                                )
                        
                        st.divider()  # Add separator between files
            
            if not show_all_main and len(excel_files) > 5:
                st.caption(f"Showing 5 of {len(excel_files)} files. Check 'Show all files' to see all.")
        else:
            st.text("No Excel files found")

def get_available_excel_files():
    """Get list of available Excel files in Excel_Statements folder"""
    excel_dir = "Excel_Statements"
    if not os.path.exists(excel_dir):
        return []
    
    excel_files = [f for f in os.listdir(excel_dir) if f.endswith('.xlsx')]
    # Sort by modification time (newest first)
    excel_files.sort(key=lambda x: os.path.getmtime(os.path.join(excel_dir, x)), reverse=True)
    return excel_files

def process_pdf_file(uploaded_file):
    """Process the uploaded PDF file and generate Excel output"""
    
    # Create progress indicators
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Save uploaded file to temporary location
        status_text.text("💾 Saving uploaded file...")
        progress_bar.progress(10)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name
        
        # Process the PDF
        status_text.text("🔄 Processing PDF and extracting financial statements...")
        progress_bar.progress(30)
        
        # Generate output filename based on uploaded file name
        base_name = os.path.splitext(uploaded_file.name)[0]
        output_filename = f"{base_name}_Statements.xlsx"
        
        # Process the annual report
        result_file = process_annual_report(tmp_file_path, output_filename)
        
        progress_bar.progress(80)
        status_text.text("✅ Processing completed!")
        
        if result_file and os.path.exists(result_file):
            progress_bar.progress(100)
            
            # Success message
            st.markdown('<div class="success-message">', unsafe_allow_html=True)
            st.success("🎉 PDF processed successfully!")
            st.markdown(f"**Generated file:** `{os.path.basename(result_file)}`")
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Download button
            with open(result_file, "rb") as file:
                st.download_button(
                    label="📥 Download Excel File",
                    data=file.read(),
                    file_name=os.path.basename(result_file),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary",
                    use_container_width=True
                )
            
            
            # Show preview of the Excel file
            show_excel_preview(result_file)
            
        else:
            st.markdown('<div class="error-message">', unsafe_allow_html=True)
            st.error("❌ Processing failed. Please check the PDF file and try again.")
            st.markdown('</div>', unsafe_allow_html=True)
    
    except Exception as e:
        st.markdown('<div class="error-message">', unsafe_allow_html=True)
        st.error(f"❌ Error processing PDF: {str(e)}")
        st.markdown('</div>', unsafe_allow_html=True)
    
    finally:
        # Clean up temporary file
        try:
            if 'tmp_file_path' in locals():
                os.unlink(tmp_file_path)
        except:
            pass
        
        # Clear progress indicators
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()

def show_all_statements_preview(excel_file_path):
    """Show a comprehensive preview of all financial statements in the Excel file"""
    try:
        # Read the Excel file
        excel_file = pd.ExcelFile(excel_file_path)
        
        # Create tabs for each sheet
        tab_names = excel_file.sheet_names
        if len(tab_names) > 0:
            tabs = st.tabs(tab_names)
            
            for i, (tab, sheet_name) in enumerate(zip(tabs, tab_names)):
                with tab:
                    try:
                        df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
                        
                        # Show sheet info
                        st.write(f"**{sheet_name}** - {len(df)} rows, {len(df.columns)} columns")
                        
                        # Show the data
                        if not df.empty:
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.info("This sheet is empty")
                            
                    except Exception as e:
                        st.error(f"Error reading sheet '{sheet_name}': {str(e)}")
        else:
            st.warning("No sheets found in the Excel file")
    
    except Exception as e:
        st.error(f"Could not preview Excel file: {str(e)}")

def show_excel_preview(excel_file_path):
    """Show a preview of the generated Excel file"""
    try:
        st.subheader("📋 Excel File Preview")
        
        # Read the Excel file
        excel_file = pd.ExcelFile(excel_file_path)
        
        # Show available sheets
        st.write("**Available Sheets:**")
        for i, sheet_name in enumerate(excel_file.sheet_names, 1):
            st.write(f"{i}. {sheet_name}")
        
        # Show preview of first sheet
        if excel_file.sheet_names:
            first_sheet = excel_file.sheet_names[0]
            df = pd.read_excel(excel_file_path, sheet_name=first_sheet)
            
            st.write(f"**Preview of '{first_sheet}' sheet:**")
            st.dataframe(df.head(10), use_container_width=True)
            
            # Show SOP metrics if available
            if "SOP_Metrics" in excel_file.sheet_names:
                st.write("**SOP Metrics Preview:**")
                sop_df = pd.read_excel(excel_file_path, sheet_name="SOP_Metrics")
                st.dataframe(sop_df.head(10), use_container_width=True)
    
    except Exception as e:
        st.warning(f"Could not preview Excel file: {str(e)}")

if __name__ == "__main__":
    main()

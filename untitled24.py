import streamlit as st
import sys
import subprocess
import os
from typing import Optional

# ========== SETUP ENVIRONMENT ========== #
def ensure_packages():
    """Install required packages if not found"""
    required = {
        'google-cloud-bigquery': 'bigquery',
        'pandas': 'pd',
        'google-auth': 'google.auth'
    }
    
    missing = []
    for pkg, alias in required.items():
        try:
            __import__(alias if alias != pkg else pkg)
        except ImportError:
            missing.append(pkg)
    
    if missing:
        st.warning(f"Menginstall package yang diperlukan: {', '.join(missing)}...")
        python = sys.executable
        try:
            subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)
            st.rerun()  # Restart app after installation
        except subprocess.CalledProcessError:
            st.error(f"Gagal menginstall {missing}. Jalankan secara manual: 'pip install {' '.join(missing)}'")
            st.stop()

ensure_packages()

# ========== MAIN IMPORTS ========== #
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json

# ========== APP CONFIGURATION ========== #
st.set_page_config(
    page_title="BigQuery Explorer",
    page_icon="🔍",
    layout="wide"
)

# ========== HELPER FUNCTIONS ========== #
@st.cache_resource
def init_bq_client(credential_info: dict) -> Optional[bigquery.Client]:
    """Initialize BigQuery client with service account credentials"""
    try:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        client = bigquery.Client(credentials=credentials)

def run_bq_query(client: bigquery.Client, query: str) -> Optional[pd.DataFrame]:
    """Execute BigQuery and return results as DataFrame"""
    try:
        job = client.query(query)
        return job.result().to_dataframe()
    except Exception as e:
        st.error(f"Query error: {str(e)}")
        return None

# ========== SIDEBAR ========== #
with st.sidebar:
    st.title("🔐 Konfigurasi")
    
    # Authentication Method
    auth_method = st.radio(
        "Metode Autentikasi",
        ["Service Account JSON", "Manual Input"]
    )
    
    creds = None
    if auth_method == "Service Account JSON":
        uploaded_file = st.file_uploader(
            "Upload File Kredensial",
            type=['json'],
            help="File JSON service account dari GCP"
        )
        if uploaded_file:
            creds = json.load(uploaded_file)
    else:
        with st.form("manual_creds"):
            project_id = st.text_input("Project ID")
            client_email = st.text_input("Client Email")
            private_key = st.text_area("Private Key", help="Masukkan seluruh teks private key termasuk BEGIN/END")
            private_key_id = st.text_input("Private Key ID")
            
            if st.form_submit_button("Gunakan Kredensial"):
                if all([project_id, client_email, private_key, private_key_id]):
                    creds = {
                        "type": "service_account",
                        "project_id": project_id,
                        "private_key_id": private_key_id,
                        "private_key": private_key,
                        "client_email": client_email,
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token",
                        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email}"
                    }
                else:
                    st.warning("Harap isi semua field kredensial")

# ========== MAIN INTERFACE ========== #
st.title("🔍 BigQuery Explorer")
tab1, tab2 = st.tabs(["Query Builder", "Custom Query"])

# Initialize client if credentials are available
client = None
if creds:
    client = init_bq_client(creds)
    if client:
        st.success(f"✅ Terhubung ke project: **{creds['project_id']}**")

# Query Builder Tab
with tab1:
    st.header("Bangun Query")
    
    if not client:
        st.warning("Silakan konfigurasi kredensial di sidebar terlebih dahulu")
        st.stop()
    
    col1, col2 = st.columns(2)
    with col1:
        dataset_id = st.text_input("Dataset ID", "your_dataset")
    with col2:
        table_id = st.text_input("Table ID", "your_table")
    
    columns = st.text_input(
        "Kolom (pisahkan koma)", 
        "*",
        help="Contoh: user_id, event_name, timestamp"
    )
    
    where_clause = st.text_input(
        "Kondisi WHERE (opsional)",
        help="Contoh: date BETWEEN '2023-01-01' AND '2023-12-31'"
    )
    
    limit = st.number_input("Limit", 1, 100000, 1000)
    
    # Build query
    query = f"""
    SELECT {columns}
    FROM `{creds['project_id']}.{dataset_id}.{table_id}`
    """
    if where_clause:
        query += f" WHERE {where_clause}"
    query += f" LIMIT {limit}"

# Custom Query Tab
with tab2:
    st.header("Query Kustom")
    custom_query = st.text_area(
        "Masukkan Query SQL", 
        height=200,
        value=query if 'query' in locals() else "SELECT * FROM `project.dataset.table` LIMIT 1000"
    )
    query = custom_query

# Display and execute query
if client:
    st.divider()
    st.subheader("Query Preview")
    st.code(query, language="sql")
    
    if st.button("🚀 Jalankan Query", type="primary"):
        with st.spinner("Menjalankan query..."):
            df = run_bq_query(client, query)
            
        if df is not None:
            st.success(f"✅ Berhasil mengambil {len(df)} baris")
            
            # Show data
            st.subheader("Hasil Query")
            st.dataframe(df, use_container_width=True)
            
            # Export options
            st.subheader("Ekspor Data")
            csv = df.to_csv(index=False).encode('utf-8')
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name="bigquery_export.csv",
                    mime="text/csv"
                )
            with col2:
                st.download_button(
                    label="📋 Salin ke Clipboard",
                    data=df.to_csv(index=False),
                    file_name="clipboard.txt",
                    mime="text/plain"
                )
            
            # Show metadata
            with st.expander("🔍 Metadata"):
                st.json({
                    "Shape": f"{df.shape[0]} baris, {df.shape[1]} kolom",
                    "Columns": list(df.columns),
                    "Dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
                })

# Footer
st.divider()
st.caption("""
**Panduan Penggunaan:**
1. Konfigurasi kredensial di sidebar (upload file JSON atau input manual)
2. Bangun query atau gunakan query kustom
3. Jalankan query dan ekspor hasilnya
""")



# import streamlit as st
# from google.oauth2 import service_account
# from google.cloud import bigquery
# import pandas as pd
# import datetime

# # Konfigurasi kredensial
# # Create API client.
# credentials = service_account.Credentials.from_service_account_info(
#     st.secrets["gcp_service_account"]
# )
# client = bigquery.Client(credentials=credentials)

# # Fungsi untuk menjalankan query
# def run_bigquery_query(query):
#     try:
#         # Membuat credentials dari dictionary
#         credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        
#         # Membuat client BigQuery
#         client = bigquery.Client(
#             credentials=credentials,
#             project=credentials.project_id,
#         )
        
#         # Menjalankan query
#         query_job = client.query(query)
#         results = query_job.result()
        
#         # Mengkonversi ke DataFrame pandas
#         df = results.to_dataframe()
        
#         return df
    
#     except Exception as e:
#         st.error(f"Error: {str(e)}")
#         return None

# # Fungsi untuk membuat query berdasarkan parameter
# def build_query(dataset, table, columns, filters=None, limit=None):
#     base_query = f"SELECT {', '.join(columns)} FROM `{dataset}.{table}`"
    
#     if filters:
#         where_clauses = []
#         for col, val in filters.items():
#             if pd.notna(val):
#                 if isinstance(val, str):
#                     where_clauses.append(f"{col} = '{val}'")
#                 elif isinstance(val, (int, float)):
#                     where_clauses.append(f"{col} = {val}")
#                 elif isinstance(val, datetime.date):
#                     where_clauses.append(f"{col} = DATE '{val}'")
#         if where_clauses:
#             base_query += " WHERE " + " AND ".join(where_clauses)
    
#     if limit:
#         base_query += f" LIMIT {limit}"
    
#     return base_query

# # Fungsi untuk mengunduh DataFrame sebagai CSV
# def download_csv(df):
#     csv = df.to_csv(index=False).encode('utf-8')
#     st.download_button(
#         label="Download data as CSV",
#         data=csv,
#         file_name='bigquery_data.csv',
#         mime='text/csv',
#     )

# # Antarmuka Streamlit
# def main():
#     st.title("BigQuery Data Explorer")
#     st.write("""
#     Aplikasi ini memungkinkan Anda untuk menarik data dari Google BigQuery berdasarkan parameter yang ditentukan 
#     dan mengekspornya ke dalam format CSV.
#     """)

#     # Pilihan dataset dan tabel
#     st.sidebar.header("Konfigurasi Koneksi")
#     dataset = st.sidebar.text_input("Nama Dataset", "bigquery-public-data.fcc_political_ads")
#     table = st.sidebar.text_input("Nama Tabel", "broadcast_tv_radio_station")

#     # Dapatkan informasi kolom
#     if st.sidebar.button("Dapatkan Informasi Kolom"):
#         try:
#             query_info = f"""
#                 SELECT column_name, data_type 
#                 FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS` 
#                 WHERE table_name = '{table.split('.')[-1]}'
#             """
#             columns_info = run_bigquery_query(query_info)
#             if columns_info is not None:
#                 st.sidebar.write("Kolom yang tersedia:")
#                 st.sidebar.dataframe(columns_info)
#         except Exception as e:
#             st.sidebar.error(f"Gagal mendapatkan informasi kolom: {str(e)}")

#     # Parameter query
#     st.header("Parameter Query")
#     col1, col2 = st.columns(2)
    
#     with col1:
#         columns_input = st.text_area(
#             "Kolom yang ingin diambil (pisahkan dengan koma)", 
#             "station_id, facility_id, call_sign, community_city"
#         )
#         columns = [col.strip() for col in columns_input.split(",") if col.strip()]
        
#         limit = st.number_input("Limit jumlah baris", min_value=1, value=1000)
    
#     with col2:
#         st.write("Filter (opsional)")
#         filter_col1 = st.text_input("Kolom Filter 1")
#         filter_val1 = st.text_input("Nilai Filter 1")
        
#         filter_col2 = st.text_input("Kolom Filter 2")
#         filter_val2 = st.text_input("Nilai Filter 2")
        
#         # Membuat dictionary filter
#         filters = {}
#         if filter_col1 and filter_val1:
#             filters[filter_col1] = filter_val1
#         if filter_col2 and filter_val2:
#             filters[filter_col2] = filter_val2

#     # Jalankan query
#     if st.button("Jalankan Query"):
#         if not columns:
#             st.warning("Silakan masukkan setidaknya satu kolom untuk diambil")
#             return
            
#         query = build_query(dataset, table, columns, filters, limit)
        
#         st.subheader("Query yang dijalankan:")
#         st.code(query, language="sql")
        
#         with st.spinner("Menjalankan query..."):
#             data = run_bigquery_query(query)
            
#             if data is not None:
#                 st.success(f"Berhasil mengambil {len(data)} baris data")
#                 st.dataframe(data)
                
#                 # Tampilkan tombol unduh
#                 download_csv(data)

# if __name__ == "__main__":
#     main()

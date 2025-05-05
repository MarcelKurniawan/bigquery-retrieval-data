import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
import os

# Konfigurasi aplikasi
st.set_page_config(page_title="BigQuery Explorer", layout="wide")
st.title("🔍 BigQuery Data Explorer")
st.markdown("""
Aplikasi untuk mengekstrak data dari Google BigQuery dan mengekspornya ke CSV
""")

# Fungsi untuk membuat koneksi ke BigQuery
@st.cache_resource
def create_bigquery_client(credentials_info):
    try:
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials_info['project_id']
        )
        return client
    except Exception as e:
        st.error(f"Gagal membuat koneksi ke BigQuery: {str(e)}")
        return None

# Fungsi untuk menjalankan query
@st.cache_data(ttl=3600)
def run_query(client, query, _credentials_info):
    try:
        query_job = client.query(query)
        results = query_job.result()
        df = results.to_dataframe()
        return df
    except Exception as e:
        st.error(f"Error menjalankan query: {str(e)}")
        return None

# Sidebar untuk konfigurasi
with st.sidebar:
    st.header("🔐 Konfigurasi BigQuery")
    
    # Input Service Account JSON
    st.subheader("Autentikasi Service Account")
    credential_json = st.text_area(
        "Tempel JSON kredensial Service Account",
        height=200,
        help="Salin seluruh isi file JSON kredensial Anda"
    )
    
    # Contoh credentials (disembunyikan)
    with st.expander("Contoh Format JSON"):
        st.code("""{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "123...456",
  "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n",
  "client_email": "bigquery-test@technicaltest-fazz.iam.gserviceaccount.com",
  "client_id": "123...789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/.../bigquery-test%40technicaltest-fazz.iam.gserviceaccount.com"
}""")

# Main panel
tab1, tab2 = st.tabs(["📋 Query Builder", "⚙️ Custom Query"])

with tab1:
    st.header("Bangun Query Anda")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        project_id = st.text_input("Project ID", "technicaltest-fazz")
    with col2:
        dataset_id = st.text_input("Dataset ID", "analytics")
    with col3:
        table_id = st.text_input("Table ID", "events")
    
    columns = st.text_input(
        "Kolom (pisahkan dengan koma)", 
        "*",
        help="Contoh: user_id, event_name, event_date"
    )
    
    where_condition = st.text_input(
        "Kondisi WHERE (opsional)",
        help="Contoh: event_date BETWEEN '2023-01-01' AND '2023-12-31'"
    )
    
    limit = st.number_input(
        "Limit data", 
        min_value=1, 
        max_value=100000, 
        value=1000
    )
    
    # Bangun query
    query = f"""
    SELECT {columns}
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    
    if where_condition:
        query += f" WHERE {where_condition}"
    
    query += f" LIMIT {limit}"

with tab2:
    st.header("Query Kustom")
    custom_query = st.text_area(
        "Masukkan query BigQuery Anda", 
        height=200,
        value=query
    )
    query = custom_query

# Tampilkan query
st.subheader("Query yang akan dijalankan")
st.code(query, language="sql")

# Jalankan query
if st.button("🚀 Jalankan Query", type="primary"):
    if not credential_json:
        st.warning("Harap masukkan kredensial Service Account")
    else:
        try:
            credentials_info = json.loads(credential_json)
            
            # Validasi client_email
            if credentials_info.get('client_email') != "bigquery-test@technicaltest-fazz.iam.gserviceaccount.com":
                st.warning("Pastikan client_email sesuai dengan service account yang dimaksud")
            
            with st.spinner("Menghubungkan ke BigQuery..."):
                client = create_bigquery_client(credentials_info)
                
                if client:
                    with st.spinner("Menjalankan query..."):
                        df = run_query(client, query, credentials_info)
                    
                    if df is not None:
                        st.success(f"✅ Berhasil mengambil {len(df)} baris data")
                        
                        # Tampilkan data
                        st.subheader("Preview Data")
                        st.dataframe(df.head(20))
                        
                        # Ekspor ke CSV
                        st.subheader("Ekspor Data")
                        csv = df.to_csv(index=False).encode('utf-8')
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(
                                label="💾 Download CSV",
                                data=csv,
                                file_name=f"bigquery_export_{table_id}.csv",
                                mime="text/csv"
                            )
                        with col2:
                            st.download_button(
                                label="📋 Copy to Clipboard",
                                data=df.to_csv(index=False),
                                file_name="clipboard.txt",
                                mime="text/plain"
                            )
                        
                        # Tampilkan metadata
                        with st.expander("🔍 Lihat Metadata"):
                            st.json({
                                "shape": df.shape,
                                "columns": list(df.columns),
                                "dtypes": dict(df.dtypes)
                            })
        except json.JSONDecodeError:
            st.error("Format JSON tidak valid. Pastikan Anda menempel seluruh isi file JSON.")
        except Exception as e:
            st.error(f"Terjadi kesalahan: {str(e)}")

# Catatan kaki
st.markdown("---")
st.caption("""
**Panduan Penggunaan:**
1. Dapatkan file JSON kredensial dari Google Cloud Console
2. Tempel seluruh isi file JSON di sidebar
3. Bangun query atau gunakan query kustom
4. Klik tombol "Jalankan Query"
5. Download hasil dalam format CSV
""")

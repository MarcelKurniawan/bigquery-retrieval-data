

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import datetime

# Konfigurasi kredensial
# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Fungsi untuk menjalankan query
def run_bigquery_query(query):
    try:
        # Membuat credentials dari dictionary
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        
        # Membuat client BigQuery
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        
        # Menjalankan query
        query_job = client.query(query)
        results = query_job.result()
        
        # Mengkonversi ke DataFrame pandas
        df = results.to_dataframe()
        
        return df
    
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return None

# Fungsi untuk membuat query berdasarkan parameter
def build_query(dataset, table, columns, filters=None, limit=None):
    base_query = f"SELECT {', '.join(columns)} FROM `{dataset}.{table}`"
    
    if filters:
        where_clauses = []
        for col, val in filters.items():
            if pd.notna(val):
                if isinstance(val, str):
                    where_clauses.append(f"{col} = '{val}'")
                elif isinstance(val, (int, float)):
                    where_clauses.append(f"{col} = {val}")
                elif isinstance(val, datetime.date):
                    where_clauses.append(f"{col} = DATE '{val}'")
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
    
    if limit:
        base_query += f" LIMIT {limit}"
    
    return base_query

# Fungsi untuk mengunduh DataFrame sebagai CSV
def download_csv(df):
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='bigquery_data.csv',
        mime='text/csv',
    )

# Antarmuka Streamlit
def main():
    st.title("BigQuery Data Explorer")
    st.write("""
    Aplikasi ini memungkinkan Anda untuk menarik data dari Google BigQuery berdasarkan parameter yang ditentukan 
    dan mengekspornya ke dalam format CSV.
    """)

    # Pilihan dataset dan tabel
    st.sidebar.header("Konfigurasi Koneksi")
    dataset = st.sidebar.text_input("Nama Dataset", "bigquery-public-data.fcc_political_ads")
    table = st.sidebar.text_input("Nama Tabel", "broadcast_tv_radio_station")

    # Dapatkan informasi kolom
    if st.sidebar.button("Dapatkan Informasi Kolom"):
        try:
            query_info = f"""
                SELECT column_name, data_type 
                FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS` 
                WHERE table_name = '{table.split('.')[-1]}'
            """
            columns_info = run_bigquery_query(query_info)
            if columns_info is not None:
                st.sidebar.write("Kolom yang tersedia:")
                st.sidebar.dataframe(columns_info)
        except Exception as e:
            st.sidebar.error(f"Gagal mendapatkan informasi kolom: {str(e)}")

    # Parameter query
    st.header("Parameter Query")
    col1, col2 = st.columns(2)
    
    with col1:
        columns_input = st.text_area(
            "Kolom yang ingin diambil (pisahkan dengan koma)", 
            "station_id, facility_id, call_sign, community_city"
        )
        columns = [col.strip() for col in columns_input.split(",") if col.strip()]
        
        limit = st.number_input("Limit jumlah baris", min_value=1, value=1000)
    
    with col2:
        st.write("Filter (opsional)")
        filter_col1 = st.text_input("Kolom Filter 1")
        filter_val1 = st.text_input("Nilai Filter 1")
        
        filter_col2 = st.text_input("Kolom Filter 2")
        filter_val2 = st.text_input("Nilai Filter 2")
        
        # Membuat dictionary filter
        filters = {}
        if filter_col1 and filter_val1:
            filters[filter_col1] = filter_val1
        if filter_col2 and filter_val2:
            filters[filter_col2] = filter_val2

    # Jalankan query
    if st.button("Jalankan Query"):
        if not columns:
            st.warning("Silakan masukkan setidaknya satu kolom untuk diambil")
            return
            
        query = build_query(dataset, table, columns, filters, limit)
        
        st.subheader("Query yang dijalankan:")
        st.code(query, language="sql")
        
        with st.spinner("Menjalankan query..."):
            data = run_bigquery_query(query)
            
            if data is not None:
                st.success(f"Berhasil mengambil {len(data)} baris data")
                st.dataframe(data)
                
                # Tampilkan tombol unduh
                download_csv(data)

if __name__ == "__main__":
    main()

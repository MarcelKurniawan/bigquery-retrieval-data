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
        
        # Mengkonversi ke DataFrame pandas dengan batching untuk menghindari memory issues
        df = query_job.to_dataframe(progress_bar_type=None)
        
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
                    where_clauses.append(f"{col} LIKE '%{val}%'")
                elif isinstance(val, (int, float)):
                    where_clauses.append(f"{col} = {val}")
                elif isinstance(val, datetime.date):
                    where_clauses.append(f"{col} = DATE '{val}'")
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
    
    if limit:
        base_query += f" LIMIT {limit}"
    
    return base_query

# Fungsi untuk mendapatkan informasi kolom
def get_column_info(dataset, table):
    try:
        query_info = f"""
            SELECT column_name, data_type 
            FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS` 
            WHERE table_name = '{table.split('.')[-1]}'
        """
        return run_bigquery_query(query_info)
    except Exception as e:
        st.error(f"Gagal mendapatkan informasi kolom: {str(e)}")
        return None

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
    columns_info = None
    if st.sidebar.button("Dapatkan Informasi Kolom"):
        columns_info = get_column_info(dataset, table)
        if columns_info is not None:
            st.sidebar.write("Kolom yang tersedia:")
            st.sidebar.dataframe(columns_info)

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
        
        # Buat filter dinamis berdasarkan kolom yang tersedia
        filters = {}
        if columns_info is not None:
            # Ambil 5 kolom pertama untuk filter (bisa disesuaikan)
            filter_columns = columns_info['column_name'].head(5).tolist()
            
            for col in filter_columns:
                col_type = columns_info[columns_info['column_name'] == col]['data_type'].values[0]
                
                if col_type in ('STRING', 'DATE', 'DATETIME', 'TIMESTAMP'):
                    val = st.text_input(f"Nilai untuk {col} (teks)", key=f"filter_{col}")
                    if val:
                        filters[col] = val
                elif col_type in ('INT64', 'NUMERIC', 'FLOAT64'):
                    val = st.number_input(f"Nilai untuk {col} (angka)", key=f"filter_{col}")
                    if val is not None:
                        filters[col] = val
                elif col_type == 'BOOL':
                    val = st.selectbox(f"Nilai untuk {col} (boolean)", 
                                      [None, 'True', 'False'], 
                                      key=f"filter_{col}")
                    if val:
                        filters[col] = val == 'True'
        else:
            st.info("Klik 'Dapatkan Informasi Kolom' di sidebar untuk melihat daftar kolom yang tersedia")

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
                st.dataframe(data.head(1000))  # Batasi tampilan untuk menghindari overload
                
                # Tampilkan tombol unduh
                download_csv(data)

if __name__ == "__main__":
    main()

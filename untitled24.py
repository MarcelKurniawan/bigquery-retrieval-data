import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import datetime

# Konfigurasi kredensial
SERVICE_ACCOUNT_JSON = {
  "type": "service_account",
  "project_id": "technicaltest-fazz",
  "private_key_id": "ad999d496c542355835993e0959aa1a5ef69b247",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCTOCy+uh2Xy/CY\nydqqcNIVK3anJcQ1DeVQtQDI2u5RyKNeVJ7eVsxTa9q7Z8OMz/Xs67nNW8jvGOn3\nIAzJP2tb3tXtb7DrTwpAcXF6tSnRjYdSCjGUEGT06Uv72Ix6SZ0FzZmXUmfcKCn/\nBZMpKUA+B+qUNz0fiKqUdKaS2lpFJKUpYGbh412yaNpc/EnPqGCGIN4XF3kpRWZk\ntKIsoR5jIaZNzrqYdUMlkgPVp0QgeMBYjTt14XPM1YkQRRSPXaUyXGqv+T27uXaS\n63cYeQTKt1WB+6/bsvmcdr71VJNIk1wqL9ottgkiOKXDC57sheeXmwjmV1JL3TV7\nX1V6lWyhAgMBAAECggEAMtDhlQYOH+7rwflu6oUd5uPFRYXCN6Pvww9vUWVMfNta\nFL31xn5EcgkZC7YR6EGCPEDTtiBX97vsSnp8H10uNBxzUE1MzrROMBdYWcg9wpDt\nJDLTkS2lg2oh6bECMFXHhxDEUtjFc9dDQ71LYhpUi/TUkkuM5B0a8DphovAO1617\nqxml1InpggpMTCKG8+g2f7cZvviZOtGTDaU4GcJqfK5Rh4ehQy/tPGmfD2pPfbHa\nugq51ywRqN9FuB25TthvJxGvtkIHiIVCHs30SOcphAsJb8O9YKx2FHbURl9QyUkQ\nDoZlo1Q2Q/4Nob7oDkew574nfbGbv1r11yzK0m2SzwKBgQDPWx1KRA6RL7DpzSuB\nWU+sYzD5fnP6qWUKea2T3INRng4m2iXRrZArK2G55HYA1J0jCYaGqiO9qu2YUTUM\ntnyGzgPbPQfKI45qSbafIwGyOGqnNOd5PtbOjru97Svp3VfazxzCpbmWHdbSHXq0\neSeMDlntc1xW2+24rl08/pRMywKBgQC1wYb+38yMad6K7XOfjaPWCU8yW6Bod4bk\nE+Z48cqy5KbgyziA9BIRJNNm5TtwB/LDfJT8gr/kThwUt7aM9W23jrBnHfU6XwEe\nPqt1TrLZYQPNP68GE5c2fW7EP22suOIq6wXq34FZ5b/9x+2/qnd2JHNPnACP3IWx\nAMk9oP0KwwKBgAumUNT1UeQyS7w2/LS9sc47nGrIAfgZQEYAZBIkl3QkbyMbqnhH\nSgxC2bC59y9AwrtPM7GpKWzkh4jBNzvJnOFt/aV5nlBrAvtRvOLf8p5ysPtH66FS\nOHtOZZncE1WGTANNE46UBQM8Fe+kHFq7W89wlvjSPGPc41Q256Ifo3FVAoGBAJa2\ny4I9ghhNEcSR8fa0NM8fGRTg9bqqoqgt+EcB+Nsuz1JIMap8uR6OMt535zmW3a9M\ndx6MLSLbwl1LmYx5V2mGYLChHuAwN3Uk8nhrsdKfp89ip3eadyEwEdGZ5w/6n1CH\nYnjIrTImWXXPe04bxMOqphO1gKHKcvG7fSQlgq6RAoGAICk7DLxMefXZ+ot/RPx3\ntAnTmaJeMMpOk5AKrQgq3VDlZa1UnxC1hrJLgIXwoUT0icN93SarPHneSwjn+v6G\nR/Mzoz1wCHclY6qC6PzJiA+sxbjkyk4oG0vSujzH3V16/q0MMpoRSyR9sTidOyQ/\naLmyeYhAwcEUR2eKH1Kral4=\n-----END PRIVATE KEY-----\n",
  "client_email": "bigquery-test@technicaltest-fazz.iam.gserviceaccount.com",
  "client_id": "105174902366283600966",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-test%40technicaltest-fazz.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# Fungsi untuk menjalankan query
def run_bigquery_query(query):
    try:
        # Membuat credentials dari dictionary
        credentials = service_account.Credentials.from_service_account_info(
            SERVICE_ACCOUNT_JSON,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        
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

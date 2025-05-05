import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
import datetime
from datetime import date

# Konfigurasi kredensial
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Fungsi untuk menjalankan query
def run_bigquery_query(query):
    try:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        query_job = client.query(query)
        df = query_job.to_dataframe(progress_bar_type=None)
        return df
    except Exception as e:
        st.error(f"Error: {str(e)}")
        return None

# Fungsi untuk membuat query
def build_query(dataset, table, columns, filters=None, limit=None, distinct=False):
    select_clause = "SELECT "
    if distinct:
        select_clause += "DISTINCT "
    select_clause += f"{', '.join(columns)} FROM `{dataset}.{table}`"
    base_query = select_clause
    
    if filters and len(filters) > 0:
        where_clauses = []
        for filter in filters:
            col = filter['column']
            operator = filter['operator']
            value = filter['value']
            
            if operator in ["IS NULL", "IS NOT NULL"]:
                where_clauses.append(f"{col} {operator}")
            elif pd.notna(value):
                if operator == "LIKE":
                    where_clauses.append(f"{col} LIKE '%{value}%'")
                elif operator == "=":
                    if isinstance(value, str):
                        where_clauses.append(f"{col} = '{value}'")
                    else:
                        where_clauses.append(f"{col} = {value}")
                elif operator == "!=":
                    if isinstance(value, str):
                        where_clauses.append(f"{col} != '{value}'")
                    else:
                        where_clauses.append(f"{col} != {value}")
                elif operator == ">":
                    where_clauses.append(f"{col} > {value}")
                elif operator == "<":
                    where_clauses.append(f"{col} < {value}")
                elif operator == ">=":
                    where_clauses.append(f"{col} >= {value}")
                elif operator == "<=":
                    where_clauses.append(f"{col} <= {value}")
                elif operator == "BETWEEN":
                    if isinstance(value, list) and len(value) == 2:
                        if isinstance(value[0], date):
                            where_clauses.append(f"{col} BETWEEN DATE '{value[0]}' AND DATE '{value[1]}'")
                        else:
                            where_clauses.append(f"{col} BETWEEN {value[0]} AND {value[1]}")
                elif operator == "IN":
                    if isinstance(value, list):
                        if all(isinstance(x, str) for x in value):
                            values_str = ", ".join([f"'{x}'" for x in value])
                        else:
                            values_str = ", ".join([str(x) for x in value])
                        where_clauses.append(f"{col} IN ({values_str})")
        
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

# Fungsi untuk mengunduh data
def download_csv(df):
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download data as CSV",
        data=csv,
        file_name='bigquery_data.csv',
        mime='text/csv',
    )

# UI untuk membuat filter
def create_filter_ui(columns_info, filter_count):
    st.subheader(f"Filter {filter_count + 1}")
    col1, col2, col3 = st.columns([3, 2, 3])
    
    with col1:
        column = st.selectbox(
            "Kolom",
            options=columns_info['column_name'].tolist(),
            key=f"col_{filter_count}"
        )
    
    with col2:
        operator = st.selectbox(
            "Operator",
            options=["=", "!=", ">", "<", ">=", "<=", "LIKE", "BETWEEN", "IN", "IS NULL", "IS NOT NULL"],
            key=f"op_{filter_count}"
        )
    
    with col3:
        col_type = columns_info[columns_info['column_name'] == column]['data_type'].values[0]
        value = None
        
        if operator in ["IS NULL", "IS NOT NULL"]:
            pass  # Tidak membutuhkan value
        elif operator == "BETWEEN":
            if col_type in ('INT64', 'NUMERIC', 'FLOAT64'):
                min_val = st.number_input("Nilai minimum", key=f"min_{filter_count}")
                max_val = st.number_input("Nilai maksimum", key=f"max_{filter_count}")
                value = [min_val, max_val]
            elif col_type in ('DATE', 'DATETIME', 'TIMESTAMP'):
                min_date = st.date_input("Tanggal awal", key=f"min_date_{filter_count}")
                max_date = st.date_input("Tanggal akhir", key=f"max_date_{filter_count}")
                value = [min_date, max_date]
        elif operator == "IN":
            input_str = st.text_input("Nilai (pisahkan dengan koma)", key=f"in_{filter_count}")
            if input_str:
                if col_type in ('INT64', 'NUMERIC', 'FLOAT64'):
                    value = [x.strip() for x in input_str.split(",")]
                    try:
                        value = [float(x) for x in value]
                    except ValueError:
                        st.error("Masukkan angka yang valid")
                else:
                    value = [x.strip() for x in input_str.split(",")]
        else:
            if col_type in ('INT64', 'NUMERIC', 'FLOAT64'):
                value = st.number_input("Nilai", key=f"val_num_{filter_count}")
            elif col_type in ('DATE', 'DATETIME', 'TIMESTAMP'):
                value = st.date_input("Tanggal", key=f"val_date_{filter_count}")
            elif col_type == 'BOOL':
                value = st.selectbox(
                    "Nilai",
                    options=[True, False],
                    key=f"val_bool_{filter_count}"
                )
            else:  # STRING
                value = st.text_input("Nilai", key=f"val_str_{filter_count}")
    
    return {
        'column': column,
        'operator': operator,
        'value': value
    }

# Antarmuka utama
def main():
    st.title("BigQuery Data Explorer")
    st.write("""
    Aplikasi ini memungkinkan Anda untuk menarik data dari Google BigQuery dengan berbagai filter.
    """)

    # Konfigurasi koneksi
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

    # Parameter query utama
    st.header("Parameter Query")
    
    # Kolom yang ingin diambil
    columns_input = st.text_area(
        "Kolom yang ingin diambil (pisahkan dengan koma)", 
        "station_id, facility_id, call_sign, community_city"
    )
    columns = [col.strip() for col in columns_input.split(",") if col.strip()]
    
    # Limit
    limit = st.number_input("Limit jumlah baris", min_value=1, value=1000)
    
    # Distinct
    distinct = st.checkbox("Gunakan DISTINCT", value=False)

    # Filter
    st.header("Filter Data")
    
    filters = []  # Inisialisasi variabel filters
    if columns_info is None:
        st.info("Klik 'Dapatkan Informasi Kolom' di sidebar terlebih dahulu")
    else:
        # Tambah filter
        num_filters = st.number_input("Jumlah filter", min_value=0, max_value=10, value=0)
        
        for i in range(num_filters):
            filters.append(create_filter_ui(columns_info, i))

    # Jalankan query
    if st.button("Jalankan Query"):
        if not columns:
            st.warning("Silakan masukkan setidaknya satu kolom untuk diambil")
            return
            
        query = build_query(dataset, table, columns, filters if num_filters > 0 else None, limit, distinct)
        
        st.subheader("Query yang dijalankan:")
        st.code(query, language="sql")
        
        with st.spinner("Menjalankan query..."):
            data = run_bigquery_query(query)
            
            if data is not None:
                st.success(f"Berhasil mengambil {len(data)} baris data")
                st.dataframe(data.head(1000))
                
                # Tombol unduh
                download_csv(data)

if __name__ == "__main__":
    main()

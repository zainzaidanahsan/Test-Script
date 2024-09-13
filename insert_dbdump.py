import pandas as pd
import mysql.connector
import numpy as np

# Baca file Excel
file_path = '/mt/ebs/script/test/Test-Script/newdatadump'
df = pd.read_excel(file_path)

# Cek nama kolom yang ada di DataFrame
print("Kolom yang tersedia:", df.columns.tolist())

# Fungsi untuk mencoba dua format tanggal
def parse_date(date_str):
    try:
        # Coba format 'dd-mm-yyyy HH:MM'
        parsed_date = pd.to_datetime(date_str, format='%d-%m-%Y %H:%M', errors='coerce')
        if pd.isna(parsed_date):
            # Jika gagal, coba format 'm/d/yyyy HH:MM'
            parsed_date = pd.to_datetime(date_str, format='%m/%d/%Y %H:%M', errors='coerce')
    except Exception as e:
        # Jika terjadi error, return NaT (Not a Time)
        parsed_date = pd.NaT
    return parsed_date

# Terapkan fungsi ke kolom 'u_closed_time'
df['u_closed_time'] = df['u_closed_time'].apply(parse_date)

# Koneksi ke MariaDB
connection = mysql.connector.connect(
    host='localhost',
    user='pmifsm',
    password='pmifsm',
    database='pmifsm',
    port=3306
)

cursor = connection.cursor()

# Sesuaikan nama tabel dan kolom sesuai dengan struktur database Anda
insert_query = """
    INSERT INTO dbdump (number, stage, u_closed_time, assigned_to, reopening_count, u_external_user_s_email, request, u_ritm_region, u_ritm_source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Loop untuk memasukkan setiap baris data dari Excel ke tabel MariaDB
for index, row in df.iterrows():
    number = row.get('number', None)
    stage = row.get('stage', None)
    u_closed_time = row.get('u_closed_time', None)
    assigned_to = row.get('assigned_to', None)
    
    # Cek apakah kolom 'u_reopen_count' ada di DataFrame
    reopening_count = row.get('u_reopen_count', None)
    if isinstance(reopening_count, float) and np.isnan(reopening_count):
        reopening_count = None

    u_external_user_s_email = row.get('u_external_user_s_email', None)
    request = row.get('request', None)
    u_ritm_region = row.get('u_ritm_region', None)
    u_ritm_source = row.get('u_ritm_source', None)

    # Debug: Print data yang akan dimasukkan
    print(f"Inserting data: number={number}, stage={stage}, u_closed_time={u_closed_time}, "
          f"assigned_to={assigned_to}, reopening_count={reopening_count}, "
          f"u_external_user_s_email={u_external_user_s_email}, request={request}, "
          f"u_ritm_region={u_ritm_region}, u_ritm_source={u_ritm_source}")

    # Masukkan data ke tabel
    cursor.execute(insert_query, (
        number if pd.notna(number) else None,
        stage if pd.notna(stage) else None,
        u_closed_time if pd.notna(u_closed_time) else None,
        assigned_to if pd.notna(assigned_to) else None,
        reopening_count,
        u_external_user_s_email if pd.notna(u_external_user_s_email) else None,
        request if pd.notna(request) else None,
        u_ritm_region if pd.notna(u_ritm_region) else None,
        u_ritm_source if pd.notna(u_ritm_source) else None
    ))

# Commit dan tutup koneksi
connection.commit()
connection.close()

print("Data berhasil diimpor ke MariaDB")

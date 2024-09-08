import pandas as pd
import mysql.connector
import numpy as np

# Baca file Excel
file_path = '/mt/ebs/fileExcel/datadump.xlsx'
df = pd.read_excel(file_path)

# Cek nama kolom yang ada di DataFrame
print("Kolom yang tersedia:", df.columns.tolist())

# Konversi format tanggal
df['u_closed_time'] = pd.to_datetime(df['u_closed_time'], format='%d-%m-%Y %H:%M', errors='coerce')

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
    INSERT INTO dbdump (number, stage, u_closed_time, assigned_to, reopening_count, u_external_user_s_email, request)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
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

    # Debug: Print data yang akan dimasukkan
    print(f"Inserting data: number={number}, stage={stage}, u_closed_time={u_closed_time}, "
          f"assigned_to={assigned_to}, reopening_count={reopening_count}, "
          f"u_external_user_s_email={u_external_user_s_email}, request={request}")

    # Masukkan data ke tabel
    cursor.execute(insert_query, (
        number if pd.notna(number) else None,
        stage if pd.notna(stage) else None,
        u_closed_time if pd.notna(u_closed_time) else None,
        assigned_to if pd.notna(assigned_to) else None,
        reopening_count,
        u_external_user_s_email if pd.notna(u_external_user_s_email) else None,
        request if pd.notna(request) else None
    ))

# Commit dan tutup koneksi
connection.commit()
connection.close()

print("Data berhasil diimpor ke MariaDB")

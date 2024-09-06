import pandas as pd
import mysql.connector

# Baca file Excel
file_path = '/mt/ebs/fileExcel/datadump.xlsx'
df = pd.read_excel(file_path)

# Bersihkan DataFrame dari nilai NaN
df = df.fillna('')  # Ganti NaN dengan string kosong, atau gunakan df.dropna() untuk menghapus baris dengan NaN

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
    # Konversi nilai NaN ke None jika diperlukan
    data = (row['number'] if pd.notna(row['number']) else None,
            row['stage'] if pd.notna(row['stage']) else None,
            row['u_closed_time'] if pd.notna(row['u_closed_time']) else None,
            row['assigned_to'] if pd.notna(row['assigned_to']) else None,
            row['reopening_count'] if pd.notna(row['reopening_count']) else None,
            row['u_external_user_s_email'] if pd.notna(row['u_external_user_s_email']) else None,
            row['request'] if pd.notna(row['request']) else None)
    cursor.execute(insert_query, data)

# Commit dan tutup koneksi
connection.commit()
connection.close()

print("Data berhasil diimpor ke MariaDB")

import pandas as pd
import mysql.connector

# Baca file Excel
file_path = '/mt/ebs/fileExcel/datadump.xlsx'
df = pd.read_excel(file_path)

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
# Misalkan tabel bernama 'dbdump' dan kolomnya yang ingin diimpor
insert_query = """
    INSERT INTO dbdump (number, stage, u_closed_time, assigned_to, u_reopen_count, u_external_user_s_email, request) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# Loop untuk memasukkan setiap baris data dari Excel ke tabel MariaDB
for index, row in df.iterrows():
    cursor.execute(insert_query, (row['number'], row['stage'], row['u_closed_time'], row['assigned_to'], row['u_reopen_count'], row['u_external_user_s_email'], row['request']))

# Commit dan tutup koneksi
connection.commit()
connection.close()

print("Data berhasil diimpor ke MariaDB")

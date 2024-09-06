import pandas as pd
import mysql.connector

# Baca file Excel
file_path = '/mt/ebs/fileExcel/datadump.xlsx'
df = pd.read_excel(file_path)

# Cek nama kolom yang ada di DataFrame
print("Kolom yang tersedia:", df.columns.tolist())

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
    INSERT INTO your_table (number, stage, u_closed_time, assigned_to, reopening_count, u_external_user_s_email, request)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# Loop untuk memasukkan setiap baris data dari Excel ke tabel MariaDB
for index, row in df.iterrows():
    number = row.get('number', None)
    stage = row.get('stage', None)
    u_closed_time = row.get('u_closed_time', None)
    assigned_to = row.get('assigned_to', None)
    
    # Cek apakah kolom 'reopening_count' ada di DataFrame
    if 'reopening_count' in df.columns:
        reopening_count = row.get('reopening_count', None)
    else:
        reopening_count = None  # Atau bisa menambahkan logika lain jika kolom tidak ada
    
    u_external_user_s_email = row.get('u_external_user_s_email', None)
    request = row.get('request', None)
    
    # Masukkan data ke tabel
    cursor.execute(insert_query, (
        number,
        stage,
        u_closed_time,
        assigned_to,
        reopening_count,
        u_external_user_s_email,
        request
    ))

# Commit dan tutup koneksi
connection.commit()
connection.close()

print("Data berhasil diimpor ke MariaDB")

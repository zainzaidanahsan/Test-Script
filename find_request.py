import mysql.connector

# Koneksi ke database
conn = mysql.connector.connect(
    host='localhost',
    user='pmifsm',
    password='pmifsm',
    database='pmifsm',
    port=3306
)

cursor = conn.cursor()

# Mengambil semua tabel
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

# Mencari nilai di setiap tabel
search_value = 'REQ0600642'
found = False

for (table_name,) in tables:
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    columns = cursor.fetchall()
    column_names = [column[0] for column in columns]
    
    # Mencari di setiap kolom
    for column_name in column_names:
        query = f"SELECT 1 FROM `{table_name}` WHERE `{column_name}` = %s LIMIT 1"
        try:
            cursor.execute(query, (search_value,))
            result = cursor.fetchone()
            if result:
                print(f"Value found in table `{table_name}`, column `{column_name}`")
                found = True
        except mysql.connector.Error as err:
            print(f"Error: {err}")

if not found:
    print("Value not found in any table or column.")

cursor.close()
conn.close()
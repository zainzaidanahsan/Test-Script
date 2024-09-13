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
for (table_name,) in tables:
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    columns = cursor.fetchall()
    column_names = [column[0] for column in columns]
    
    # Mencari di setiap kolom
    for column_name in column_names:
        query = f"SELECT * FROM `{table_name}` WHERE `{column_name}` = %s"
        try:
            cursor.execute(query, (search_value,))
            results = cursor.fetchall()
            if results:
                print(f"Found in table `{table_name}`, column `{column_name}`:")
                for row in results:
                    print(row)
        except mysql.connector.Error as err:
            print(f"Error: {err}")

cursor.close()
conn.close()
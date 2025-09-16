# check_db.py
import pyodbc
import getpass

# --- Verifique se estes dados estão corretos ---
DB_SERVER = '172.16.1.223'
DB_INVENTARIO = 'inventario'
DB_USERNAME = 'sa'
# ----------------------------------------------

print("A tentar conectar à base de dados...")

try:
    # Pedir a senha para não a deixar escrita no código
    db_password = getpass.getpass(prompt=f"Digite a senha do utilizador '{DB_USERNAME}' para o SQL Server: ")

    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={DB_SERVER};'
        f'DATABASE={DB_INVENTARIO};'
        f'UID={DB_USERNAME};'
        f'PWD={db_password};'
        f'TrustServerCertificate=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Conexão bem-sucedida!")
    print("-" * 30)

    # Vamos buscar os 5 registos mais recentes da tabela de detalhes
    query = """
    SELECT TOP 5 id, log_id, item_code, filial, local 
    FROM sync_log_details 
    ORDER BY id DESC
    """
    
    print("A executar a consulta...")
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    print("Resultados da consulta:")
    if not results:
        print("Nenhum registo encontrado na tabela sync_log_details.")
    else:
        for row in results:
            print(f"ID: {row.id}, Log_ID: {row.log_id}, Item: {row.item_code}, Filial: '{row.filial}', Local: '{row.local}'")

except pyodbc.Error as ex:
    print(f"\nERRO: Não foi possível conectar ou consultar a base de dados.")
    print(f"Detalhes do erro: {ex}")

except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")
# fix_permissions.py
import pyodbc
from app import DB_INVENTARIO, DB_USERNAME, DB_SERVER
from setup_database import get_password
import os

# --- LISTA COMPLETA DE PERMISSÕES DO SEU PROJETO ---
# Verifique e adicione qualquer outra permissão se necessário
AVAILABLE_MENUS = [
    'dashboard', 'contagem_consolidada_page', 'contagem_planejada', 'lista_recontagem',
    'gerar_etiquetas', 'gerenciar_inventarios', 'selecionar_progress_dashboard',
    'selecionar_inventario_analitico', 'analise_estoque_parado', 'admin', 'manage_roles', 'view_logs',
    'edit_user', 'reset_password', 'delete_user'
]

print("A tentar reconectar à base de dados para corrigir as permissões...")

try:
    password = get_password()
    if not password:
        print("Senha não fornecida. Encerrando.")
        exit()

    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'\
        f'SERVER={DB_SERVER};'\
        f'DATABASE={DB_INVENTARIO};'\
        f'UID={DB_USERNAME};'\
        f'PWD={password};'\
        f'TrustServerCertificate=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # 1. Encontra o ID do perfil 'Admin' usando o nome da coluna CORRETO
    cursor.execute("SELECT id FROM roles WHERE name = 'Admin'")
    admin_role_id = cursor.fetchone()[0]
    
    if admin_role_id:
        # 2. Apaga quaisquer permissões existentes para o Admin
        cursor.execute("DELETE FROM role_permissions WHERE role_id = ?", admin_role_id)
        
        # 3. Insere todas as permissões completas novamente
        permissions_to_insert = [(admin_role_id, endpoint) for endpoint in AVAILABLE_MENUS]
        cursor.executemany("INSERT INTO role_permissions (role_id, menu_endpoint) VALUES (?, ?)", permissions_to_insert)
        conn.commit()
        
        print("Permissões do perfil 'Admin' restauradas com sucesso!")
        print("Você pode fazer login novamente e ter acesso a todas as páginas.")
        
    else:
        print("Perfil 'Admin' não encontrado no banco de dados.")

except pyodbc.Error as ex:
    print(f"Erro ao conectar ou executar a consulta no banco de dados: {ex}")
finally:
    if 'conn' in locals():
        conn.close()
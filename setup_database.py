# setup_database.py
# -*- coding: utf-8 -*-

import pyodbc
from werkzeug.security import generate_password_hash
import getpass
from werkzeug.security import generate_password_hash # Garanta que esta importação está presente

# --- Configurações de Conexão ---
SERVER = '172.16.1.223'
USERNAME = 'sa'
DATABASE_TO_CREATE = 'inventario'

# --- INFORMAÇÕES DO NOVO USUÁRIO MASTER ---
MASTER_USERNAME = 'master'
# **Importante:** Altere 'masterpassword' por uma senha segura.
# Você pode alterá-la novamente após o primeiro login.
MASTER_PASSWORD = 'master' 

# --- LISTA COMPLETA DE PERMISSÕES ---
# Garante que o usuário master terá acesso a todas as páginas.
AVAILABLE_MENUS = [
    'dashboard', 'contagem_consolidada_page', 'contagem_planejada', 'lista_recontagem',
    'gerar_etiquetas', 'gerenciar_inventarios', 'selecionar_progress_dashboard',
    'selecionar_inventario_analitico', 'analise_estoque_parado', 'admin', 'manage_roles', 'view_logs',
    'edit_user', 'reset_password', 'delete_user'
]

# --- Funções Auxiliares ---
def get_password(prompt="Digite a senha do SQL Server: "):
    try:
        return getpass.getpass(prompt)
    except Exception as error:
        print('ERRO', error)
        return None

def create_connection_to_server(password):
    try:
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={SERVER};'
            f'UID={USERNAME};'
            f'PWD={password};'
            f'TrustServerCertificate=yes;'
        )
        conn = pyodbc.connect(conn_str, autocommit=True)
        print("Conexão com o servidor SQL estabelecida com sucesso.")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Erro ao conectar ao SQL Server: {sqlstate}")
        return None

def create_database(cursor, db_name):
    try:
        cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{db_name}') CREATE DATABASE {db_name};")
        print(f"Banco de dados '{db_name}' verificado/criado com sucesso.")
    except pyodbc.Error as ex:
        print(f"Erro ao criar o banco de dados '{db_name}': {ex}")
        raise

def create_connection_to_db(password, db_name):
    try:
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={SERVER};'
            f'DATABASE={db_name};'
            f'UID={USERNAME};'
            f'PWD={password};'
            f'TrustServerCertificate=yes;'
        )
        conn = pyodbc.connect(conn_str)
        print(f"Conexão com o banco de dados '{db_name}' estabelecida com sucesso.")
        return conn
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Erro ao conectar ao banco de dados '{db_name}': {sqlstate}")
        return None

def create_tables(cursor):
    """Cria e atualiza TODAS as tabelas do sistema de forma segura."""
    try:
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='usuarios' and xtype='U')
        CREATE TABLE usuarios (
            id INT PRIMARY KEY IDENTITY(1,1),
            username NVARCHAR(50) NOT NULL UNIQUE,
            password_hash NVARCHAR(255) NOT NULL,
            filial NVARCHAR(10),
            role NVARCHAR(20),
            pode_ver_saldo BIT DEFAULT 1,
            D_E_L_E_T_ NCHAR(1) DEFAULT ''
        );
        """)
        print("Tabela 'usuarios' verificada/criada com sucesso.")

        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='roles' and xtype='U')
        CREATE TABLE roles (
            id INT PRIMARY KEY IDENTITY(1,1),
            name NVARCHAR(50) NOT NULL UNIQUE,
            D_E_L_E_T_ NCHAR(1) DEFAULT ''
        );
        """)
        print("Tabela 'roles' verificada/criada com sucesso.")
        
        # ---------------------------------------------
        # --- NOVO: AÇÃO PARA GARANTIR A COLUNA 'NAME' ---
        # ---------------------------------------------
        # Verifica se a coluna 'name' já existe na tabela 'roles'
        try:
            cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'roles' AND COLUMN_NAME = 'name'")
            if cursor.fetchone() is None:
                print("Coluna 'name' não encontrada em 'roles'. Adicionando...")
                cursor.execute("ALTER TABLE roles ADD name NVARCHAR(50) NULL")
                print("Coluna 'name' adicionada com sucesso.")
            else:
                print("A coluna 'name' já existe em 'roles'. Nenhuma alteração é necessária.")
                
        except pyodbc.Error as ex:
            print(f"Erro ao verificar/adicionar a coluna 'name' à tabela 'roles': {ex}")
            raise # Levanta o erro para interromper a execução


        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE Name = N'role_id' AND Object_ID = Object_ID(N'usuarios'))
        ALTER TABLE usuarios ADD role_id INT NULL;
        """)
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_usuarios_roles')
        ALTER TABLE usuarios ADD CONSTRAINT FK_usuarios_roles FOREIGN KEY (role_id) REFERENCES roles(id);
        """)
        print("Tabela 'usuarios' verificada/atualizada com sucesso.")

        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='role_permissions' and xtype='U')
        CREATE TABLE role_permissions (
            id INT PRIMARY KEY IDENTITY(1,1),
            role_id INT NOT NULL,
            menu_endpoint NVARCHAR(100) NOT NULL,
            FOREIGN KEY (role_id) REFERENCES roles(id),
            UNIQUE (role_id, menu_endpoint)
        );
        """)
        print("Tabela 'role_permissions' verificada/criada com sucesso.")

        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='inventario_sessoes' and xtype='U')
        CREATE TABLE inventario_sessoes (
            id INT PRIMARY KEY IDENTITY(1,1),
            descricao NVARCHAR(255) NOT NULL,
            data_abertura DATETIME DEFAULT GETDATE(),
            data_fechamento DATETIME NULL,
            status NVARCHAR(20) DEFAULT 'Aberto',
            usuario_id_abertura INT NOT NULL,
            D_E_L_E_T_ NCHAR(1) DEFAULT '',
            FOREIGN KEY (usuario_id_abertura) REFERENCES usuarios(id)
        );
        """)
        print("Tabela 'inventario_sessoes' verificada/criada com sucesso.")

        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='contagens' and xtype='U')
        CREATE TABLE contagens (
            id INT PRIMARY KEY IDENTITY(1,1), sessao_id INT NOT NULL, usuario_id INT NOT NULL,
            codigo_item NVARCHAR(100) NOT NULL, filial NVARCHAR(10) NOT NULL, local NVARCHAR(10) NOT NULL,
            saldo_sistema DECIMAL(18, 5) NOT NULL, quantidade_contada DECIMAL(18, 5) NOT NULL,
            data_contagem DATETIME DEFAULT GETDATE(), status_contagem NVARCHAR(30) DEFAULT 'OK',
            contagem_pai_id INT NULL, numero_contagem INT NOT NULL DEFAULT 1,
            FOREIGN KEY (sessao_id) REFERENCES inventario_sessoes(id),
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
            FOREIGN KEY (contagem_pai_id) REFERENCES contagens(id)
        );
        """)
        print("Tabela 'contagens' verificada/atualizada com sucesso.")
    
        # Garante que os perfis básicos existam com nome
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM roles WHERE name = 'Admin') INSERT INTO roles (name) VALUES ('Admin')")
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM roles WHERE name = 'Supervisor') INSERT INTO roles (name) VALUES ('Supervisor')")
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM roles WHERE name = 'Operador') INSERT INTO roles (name) VALUES ('Operador')")

    except pyodbc.Error as ex:
        print(f"Erro ao criar/atualizar tabelas: {ex}")
        raise

def setup_initial_roles_and_permissions(cursor):
    """Cria os perfis padrão, migra os usuários e define as permissões do Admin."""
    try:
        # Garante que o perfil 'Admin' exista
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM roles WHERE name = 'Admin') INSERT INTO roles (name) VALUES ('Admin')")
        
        # Seleciona o ID do perfil 'Admin' usando o nome da coluna CORRETO
        cursor.execute("SELECT id FROM roles WHERE name = 'Admin'")
        admin_role_id = cursor.fetchone()[0]
        
        # --- NOVO: Limpa as permissões existentes do Admin antes de redefinir ---
        cursor.execute("DELETE FROM role_permissions WHERE role_id = ?", admin_role_id)
        
        # Lista de todos os menus e ações disponíveis
        all_menus = [
            'dashboard', 'contagem_consolidada_page', 'contagem_planejada', 'lista_recontagem',
            'gerar_etiquetas', 'gerenciar_inventarios', 'selecionar_progress_dashboard',
            'selecionar_inventario_analitico', 'analise_estoque_parado', 'admin', 'manage_roles', 'view_logs',
            'edit_user', 'reset_password', 'delete_user'
        ]
        
        # Insere todas as permissões para o Admin
        permissions_to_insert = [(admin_role_id, endpoint) for endpoint in all_menus]
        cursor.executemany("INSERT INTO role_permissions (role_id, menu_endpoint) VALUES (?, ?)", permissions_to_insert)

        print("Permissões padrão do perfil Admin foram definidas e atualizadas.")
        
    except pyodbc.Error as ex:
        print(f"Erro ao configurar perfis iniciais: {ex}")
        raise

def create_admin_user(cursor):
    """Cria ou atualiza um usuário administrador padrão."""
    try:
        # Garante que o perfil 'Admin' exista
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM roles WHERE name = 'Admin') INSERT INTO roles (name) VALUES ('Admin')")
        
        # Seleciona o ID do perfil 'Admin' usando o nome da coluna CORRETO
        cursor.execute("SELECT id FROM roles WHERE name = 'Admin'")
        admin_role_id = cursor.fetchone()[0]

        cursor.execute("SELECT id FROM usuarios WHERE username = 'admin' AND D_E_L_E_T_ <> '*'")
        if cursor.fetchone() is None:
            admin_password = get_password("O usuário 'admin' não foi encontrado. Digite uma nova senha para ele: ")
            if admin_password:
                hashed_password = generate_password_hash(admin_password)
                cursor.execute(
                    "INSERT INTO usuarios (username, password_hash, filial, role_id, pode_ver_saldo) VALUES (?, ?, ?, ?, ?)",
                    'admin', hashed_password, '99', admin_role_id, 1
                )
                print("Usuário 'admin' criado com sucesso.")
        else:
            cursor.execute("UPDATE usuarios SET role_id = ?, pode_ver_saldo = 1 WHERE username = 'admin'", admin_role_id)
            print("Perfil do usuário 'admin' verificado/atualizado.")
            
    except pyodbc.Error as ex:
        print(f"Erro ao criar/atualizar usuário admin: {ex}")
        raise

def force_user_permissions(cursor, username):
    """
    Força todas as permissões para o perfil associado a um usuário específico.
    """
    try:
        # Encontra o ID do perfil associado ao usuário
        cursor.execute("SELECT role_id FROM usuarios WHERE username = ?", username)
        result = cursor.fetchone()
        
        if not result:
            print(f"Usuário '{username}' não encontrado ou não tem um perfil associado.")
            return

        user_role_id = result[0]
        
        # Lista de todos os menus e ações disponíveis
        all_menus = [
            'dashboard', 'contagem_consolidada_page', 'contagem_planejada', 'lista_recontagem',
            'gerar_etiquetas', 'gerenciar_inventarios', 'selecionar_progress_dashboard',
            'selecionar_inventario_analitico', 'analise_estoque_parado', 'admin', 'manage_roles', 'view_logs',
            'edit_user', 'reset_password', 'delete_user'
        ]
        
        # Apaga quaisquer permissões existentes para o perfil dele
        cursor.execute("DELETE FROM role_permissions WHERE role_id = ?", user_role_id)
        
        # Insere todas as permissões completas novamente
        permissions_to_insert = [(user_role_id, endpoint) for endpoint in all_menus]
        cursor.executemany("INSERT INTO role_permissions (role_id, menu_endpoint) VALUES (?, ?)", permissions_to_insert)
        
        print(f"Permissões do usuário '{username}' restauradas com sucesso.")

    except pyodbc.Error as ex:
        print(f"Erro ao forçar permissões para o usuário '{username}': {ex}")
        raise


def create_master_user_and_permissions(cursor):
    """
    Cria ou atualiza o usuário 'master' com o perfil 'Admin' e
    garante que as permissões do perfil Admin estejam completas.
    """
    try:
        # Garante que o perfil 'Admin' exista e obtém seu ID
        cursor.execute("IF NOT EXISTS (SELECT 1 FROM roles WHERE name = 'Admin') INSERT INTO roles (name) VALUES ('Admin')")
        cursor.execute("SELECT id FROM roles WHERE name = 'Admin'")
        admin_role_id = cursor.fetchone()[0]

        # Apaga e reinsere as permissões para o perfil 'Admin' (garantia de correção)
        cursor.execute("DELETE FROM role_permissions WHERE role_id = ?", admin_role_id)
        permissions_to_insert = [(admin_role_id, endpoint) for endpoint in AVAILABLE_MENUS]
        cursor.executemany("INSERT INTO role_permissions (role_id, menu_endpoint) VALUES (?, ?)", permissions_to_insert)

        # Cria ou atualiza o usuário 'master'
        cursor.execute("SELECT id FROM usuarios WHERE username = ?", MASTER_USERNAME)
        if not cursor.fetchone():
            hashed_password = generate_password_hash(MASTER_PASSWORD)
            cursor.execute(
                "INSERT INTO usuarios (username, password_hash, filial, role_id, pode_ver_saldo) VALUES (?, ?, ?, ?, ?)",
                MASTER_USERNAME, hashed_password, '99', admin_role_id, 1
            )
            print(f"Usuário '{MASTER_USERNAME}' criado com sucesso.")
        else:
            hashed_password = generate_password_hash(MASTER_PASSWORD)
            cursor.execute("UPDATE usuarios SET password_hash = ?, role_id = ?, pode_ver_saldo = 1 WHERE username = ?",
                            hashed_password, admin_role_id, MASTER_USERNAME)
            print(f"Usuário '{MASTER_USERNAME}' atualizado com sucesso. A senha foi redefinida.")

    except pyodbc.Error as ex:
        print(f"Erro ao criar/atualizar usuário master: {ex}")
        raise


# --- Execução Principal ---
if __name__ == "__main__":
    password = get_password()
    if not password:
        print("Senha não fornecida. Encerrando.")
    else:
        server_conn = create_connection_to_server(password)
        if server_conn:
            cursor = server_conn.cursor()
            try:
                create_database(cursor, DATABASE_TO_CREATE)
                db_conn = create_connection_to_db(password, DATABASE_TO_CREATE)
                if db_conn:
                    db_cursor = db_conn.cursor()
                    create_tables(db_cursor)
                    db_conn.commit()
                    
                    # Chamada para a nova função que cria o master
                    create_master_user_and_permissions(db_cursor)
                    
                    db_conn.commit()
                    db_cursor.close()
                    db_conn.close()
                    print("\nConfiguração do banco de dados concluída com sucesso!")
            except Exception as e:
                print(f"Ocorreu um erro geral durante a configuração: {e}")
            finally:
                cursor.close()
                server_conn.close()
                print("Conexão com o servidor fechada.")
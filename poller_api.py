# poller_api.py
# -*- coding: utf-8 -*-

import pyodbc
from datetime import datetime, timedelta
import time
import requests
import json

# --- CONFIGURAÇÕES DE CONEXÃO ---
SOURCE_SERVER = '172.16.1.218'
SOURCE_DATABASE = 'P12_PROD'
SOURCE_USERNAME = 'totvs'
SOURCE_PASSWORD = 'totvs@1010'

DEST_SERVER = '172.16.1.223'
DEST_DATABASE = 'inventario' # Base de dados onde está a tabela de logs
DEST_USERNAME = 'sa'
DEST_PASSWORD = 'Rp@T3ch#50'

RECEIVER_API_URL = 'http://172.16.1.223:5002/api/sync_balances'
SYNC_SECRET_TOKEN = "Rp@T3ch#50"
POLL_INTERVAL = 10 

# --- NOME DO FICHEIRO DE CONTROLO ---
LAST_SYNC_FILE = 'last_sync.txt'

# --- Funções de Controlo de Sincronização ---
def read_last_sync_time():
    """Lê a data e hora da última sincronização bem-sucedida do ficheiro de controlo."""
    try:
        with open(LAST_SYNC_FILE, 'r') as f:
            content = f.read().strip()
            if content and ';' in content:
                last_date, last_time = content.split(';')
                print(f"INFO: A retomar sincronização a partir de {last_date} {last_time}.")
                return last_date, last_time
    except FileNotFoundError:
        print("INFO: Ficheiro 'last_sync.txt' não encontrado. Será feita uma verificação inicial completa.")
    except Exception as e:
        print(f"AVISO: Erro ao ler 'last_sync.txt': {e}. A recomeçar do início.")
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    return yesterday, '00:00:00'

def write_last_sync_time(last_date, last_time):
    """Escreve a data e hora da última sincronização no ficheiro de controlo."""
    try:
        with open(LAST_SYNC_FILE, 'w') as f:
            f.write(f"{last_date};{last_time}")
    except Exception as e:
        log_to_db('ERROR', f"FALHA CRÍTICA ao escrever em last_sync.txt: {e}")

# --- FUNÇÃO DE LOGGING NO BANCO DE DADOS (sem alterações) ---
def log_to_db(level, message):
    try:
        conn_log = get_db_connection(DEST_SERVER, DEST_DATABASE, DEST_USERNAME, DEST_PASSWORD)
        if conn_log:
            cursor_log = conn_log.cursor()
            cursor_log.execute(
                "INSERT INTO sync_logs (log_level, source, message) VALUES (?, ?, ?)",
                level, 'PollerAPI (218)', message
            )
            conn_log.commit()
            conn_log.close()
    except Exception as e:
        print(f"[{datetime.now()}] FALHA CRÍTICA AO GRAVAR LOG: {e}")

# --- FUNÇÃO DE CONEXÃO (sem alterações) ---
def get_db_connection(server, database, username, password):
    try:
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            f'TrustServerCertificate=yes;'
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as ex:
        log_to_db('ERROR', f"ERRO de conexão com '{server}/{database}': {ex}")
        return None

def read_last_sync_time():
    try:
        with open(LAST_SYNC_FILE, 'r') as f:
            content = f.read().strip()
            if content and ';' in content:
                last_date, last_time = content.split(';')
                print(f"INFO: A retomar sincronização a partir de {last_date} {last_time}.")
                return last_date, last_time
    except FileNotFoundError:
        print("INFO: Ficheiro 'last_sync.txt' não encontrado. Será feita uma verificação inicial completa.")
    except Exception as e:
        print(f"AVISO: Erro ao ler 'last_sync.txt': {e}. A recomeçar do início.")
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    return yesterday, '00:00:00'
def write_last_sync_time(last_date, last_time):
    try:
        with open(LAST_SYNC_FILE, 'w') as f:
            f.write(f"{last_date};{last_time}")
    except Exception as e:
        log_to_db('ERROR', f"FALHA CRÍTICA ao escrever em last_sync.txt: {e}")
def log_to_db(level, message):
    """Grava uma mensagem de log na tabela sync_logs."""
    try:
        conn_log = get_db_connection(DEST_SERVER, DEST_DATABASE, DEST_USERNAME, DEST_PASSWORD)
        if conn_log:
            cursor_log = conn_log.cursor()
            # ALTERAÇÃO: O texto agora deixa claro a origem dos dados
            source_text = f"Poller (Exec: {DEST_SERVER}, Origem: {SOURCE_SERVER})"
            
            cursor_log.execute(
                "INSERT INTO sync_logs (log_level, source, message) VALUES (?, ?, ?)",
                level, source_text, message
            )
            conn_log.commit()
            conn_log.close()
    except Exception as e:
        print(f"[{datetime.now()}] FALHA CRÍTICA AO GRAVAR LOG: {e}")

def get_db_connection(server, database, username, password):
    try:
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            f'TrustServerCertificate=yes;'
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as ex:
        log_to_db('ERROR', f"ERRO de conexão com '{server}/{database}': {ex}")
        return None


# --- FUNÇÃO PRINCIPAL DE VERIFICAÇÃO (LÓGICA ATUALIZADA) ---
def read_last_sync_time():
    try:
        with open(LAST_SYNC_FILE, 'r') as f:
            content = f.read().strip()
            if content and ';' in content:
                last_date, last_time = content.split(';')
                print(f"INFO: A retomar sincronização a partir de {last_date} {last_time}.")
                return last_date, last_time
    except FileNotFoundError:
        print("INFO: Ficheiro 'last_sync.txt' não encontrado. Será feita uma verificação inicial completa.")
    except Exception as e:
        print(f"AVISO: Erro ao ler 'last_sync.txt': {e}. A recomeçar do início.")
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    return yesterday, '00:00:00'
def write_last_sync_time(last_date, last_time):
    try:
        with open(LAST_SYNC_FILE, 'w') as f:
            f.write(f"{last_date};{last_time}")
    except Exception as e:
        log_to_db('ERROR', f"FALHA CRÍTICA ao escrever em last_sync.txt: {e}")
def log_to_db(level, message):
    """Grava uma mensagem de log na tabela sync_logs."""
    try:
        conn_log = get_db_connection(DEST_SERVER, DEST_DATABASE, DEST_USERNAME, DEST_PASSWORD)
        if conn_log:
            cursor_log = conn_log.cursor()
            # O texto da origem agora é dinâmico
            source_text = f"Poller (Exec: {DEST_SERVER}, Origem: {SOURCE_SERVER})"

            cursor_log.execute(
                "INSERT INTO sync_logs (log_level, source, message) VALUES (?, ?, ?)",
                level, source_text, message
            )
            conn_log.commit()
            conn_log.close()
    except Exception as e:
        print(f"[{datetime.now()}] FALHA CRÍTICA AO GRAVAR LOG: {e}")
def get_db_connection(server, database, username, password):
    try:
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            f'TrustServerCertificate=yes;'
        )
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as ex:
        log_to_db('ERROR', f"ERRO de conexão com '{server}/{database}': {ex}")
        return None

# --- FUNÇÃO PRINCIPAL DE VERIFICAÇÃO (LÓGICA ATUALIZADA) ---
def start_polling_service():
    log_to_db('INFO', "--- Serviço de Verificação de Saldo Iniciado ---")
    print("--- Serviço de Verificação de Saldo Iniciado ---")
    
    last_sync_date, last_sync_time = read_last_sync_time()

    while True:
        try:
            conn_source = get_db_connection(SOURCE_SERVER, SOURCE_DATABASE, SOURCE_USERNAME, SOURCE_PASSWORD)
            if not conn_source:
                time.sleep(POLL_INTERVAL)
                continue

            cursor_source = conn_source.cursor()
            
            # --- QUERY ATUALIZADA PARA INCLUIR FILIAL E LOCAL ---
            query_source = """
            SELECT 
                b2.B2_COD, 
                b2.B2_QATU, 
                b2.B2_DMOV, 
                b2.B2_HMOV,
                b2.B2_FILIAL,
                b2.B2_LOCAL,
                ISNULL(b1.B1_DESC, 'N/A') as B1_DESC
            FROM 
                SB2010 b2
            LEFT JOIN 
                SB1010 b1 ON b2.B2_COD = b1.B1_COD AND b1.D_E_L_E_T_ <> '*'
            WHERE 
                b2.D_E_L_E_T_ <> '*' 
                AND (b2.B2_DMOV > ? OR (b2.B2_DMOV = ? AND b2.B2_HMOV > ?))
            ORDER BY 
                b2.B2_DMOV, b2.B2_HMOV
            """
            cursor_source.execute(query_source, last_sync_date, last_sync_date, last_sync_time)
            
            all_items = cursor_source.fetchall()

            if all_items:
                # --- PAYLOAD ATUALIZADO PARA INCLUIR FILIAL E LOCAL ---
                items_to_update = [
                    {
                        "code": row.B2_COD.strip(), 
                        "balance": float(row.B2_QATU),
                        "description": row.B1_DESC.strip(),
                        "filial": row.B2_FILIAL.strip(),
                        "local": row.B2_LOCAL.strip()
                    }
                    for row in all_items
                ]
                
                sample_codes = ', '.join([f"{item['code']}({item['filial']}-{item['local']})" for item in items_to_update[:3]])
                log_to_db('INFO', f"Detetadas {len(items_to_update)} novas alterações. Amostra: [{sample_codes}...]. Enviando...")
                
                headers = {'Content-Type': 'application/json', 'X-Sync-Token': SYNC_SECRET_TOKEN}
                
                try:
                    response = requests.post(RECEIVER_API_URL, data=json.dumps(items_to_update), headers=headers, timeout=30)
                    response.raise_for_status()
                    
                    last_item_processed = all_items[-1]
                    new_last_date = last_item_processed.B2_DMOV.strip()
                    new_last_time = last_item_processed.B2_HMOV.strip()
                    
                    write_last_sync_time(new_last_date, new_last_time)
                    log_to_db('INFO', f"Sincronização bem-sucedida. Ponto de controlo atualizado para {new_last_date} {new_last_time}.")

                    last_sync_date = new_last_date
                    last_sync_time = new_last_time

                except requests.exceptions.RequestException as e:
                    log_to_db('ERROR', f"ERRO ao enviar dados para a API: {e}. A tentar novamente no próximo ciclo.")

            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Nenhuma alteração nova encontrada.")

            if conn_source:
                conn_source.close()

        except Exception as e:
            log_to_db('ERROR', f"Ocorreu um erro inesperado no loop principal: {e}")
        
        time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    start_polling_service()
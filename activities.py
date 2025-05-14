import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime, timezone
import time
import pyodbc
import json
import numpy as np
import re
import os

"""Config dotenv"""
from dotenv import load_dotenv
from pathlib import Path
def localizar_env(diretorio_raiz="PRIVATE_BAG.ENV"):
    path = Path(__file__).resolve()
    for parent in path.parents:
        possible = parent / diretorio_raiz / ".env"
        if possible.exists():
            return possible
    raise FileNotFoundError(f"Arquivo .env não encontrado dentro de '{diretorio_raiz}'.")
env_path = localizar_env()
load_dotenv(dotenv_path=env_path)

# Dados de autenticação
email_address = os.getenv('ZENDESK_EMAIL')
api_token = os.getenv('ZENDESK_TOKEN')
auth = HTTPBasicAuth(f'{email_address}/token', api_token)

# Configuração do banco de dados
db_config = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': f"{os.getenv('DB_SERVER_EXCEL')},{os.getenv('DB_PORT_EXCEL')}",
    'database': os.getenv('DB_DATABASE_EXCEL'),
    'uid': os.getenv('DB_USER_EXCEL'),
    'pwd': os.getenv('DB_PASSWORD_EXCEL')
}

def buscar_atividades():
    """
    Busca todas as atividades dos últimos 30 dias na API do Zendesk
    (o endpoint /activities só guarda 30 dias).
    Faz paginação sequencial até não haver next_page.
    Retorna uma lista de dict (json).
    """
    url = 'https://bagaggio.zendesk.com/api/v2/activities'  # sem parâmetro 'since'
    atividades_data = []
    page_count = 1

    while url:
        print(f"Buscando página {page_count} -> {url}")
        response = requests.get(url, auth=auth)
        if response.status_code != 200:
            print(f'Erro ao buscar atividades: {response.status_code}')
            print(f'Mensagem da API: {response.text}')
            break

        data = response.json()
        atividades = data.get('activities', [])
        print(f'Atividades nesta página: {len(atividades)}')

        atividades_data.extend(atividades)
        url = data.get('next_page')  # Será None/null quando acabar
        page_count += 1
        time.sleep(1)  # Pausa para evitar sobrecarga

    print(f"Total de atividades coletadas: {len(atividades_data)}")
    return atividades_data

def tratar_dados(atividades_data):
    """
    Converte lista de dict em DataFrame e ajusta colunas,
    datas e estrutura para inserir no banco ou exportar.
    """
    try:
        df = pd.DataFrame(atividades_data)
        if df.empty:
            print("Nenhum dado para tratar.")
            return df

        # ----------------------- TRATAR DATAS ---------------------------------
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').dt.tz_localize(None)
        if 'updated_at' in df.columns:
            df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce').dt.tz_localize(None)

        min_dt = pd.Timestamp('1753-01-01')
        max_dt = pd.Timestamp('9999-12-31')

        if 'created_at' in df.columns:
            df.loc[
                (df['created_at'].isna()) | (df['created_at'] < min_dt) | (df['created_at'] > max_dt),
                'created_at'
            ] = None

        if 'updated_at' in df.columns:
            df.loc[
                (df['updated_at'].isna()) | (df['updated_at'] < min_dt) | (df['updated_at'] > max_dt),
                'updated_at'
            ] = None

        # Extraindo dados do ator
        if 'actor' in df.columns:
            df['actor_id'] = df['actor'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
            df['actor_name'] = df['actor'].apply(lambda x: x.get('name') if isinstance(x, dict) else None)

        # Informações do ticket (target)
        if 'target' in df.columns:
            df['ticket_id'] = df['target'].apply(
                lambda x: str(x.get('id')) if isinstance(x, dict) and 'id' in x else None
            )
            df['ticket_type'] = df['target'].apply(
                lambda x: x.get('type') if isinstance(x, dict) else None
            )

        # verb -> action
        if 'verb' in df.columns:
            df['action'] = df['verb']
        if 'url' in df.columns:
            df['activity_url'] = df['url']

        # object -> comment, subject, público
        if 'object' in df.columns:
            df['comment'] = df['object'].apply(
                lambda x: x.get('comment', {}).get('value') if isinstance(x, dict) else None
            )
            df['subject'] = df['object'].apply(
                lambda x: x.get('ticket', {}).get('subject') if isinstance(x, dict) else None
            )
            df['público'] = df['object'].apply(
                lambda x: x.get('comment', {}).get('public') if isinstance(x, dict) else None
            )
            df['público'] = df['público'].astype(str)

        # Extrair ticket_id da coluna 'title' se não existir
        if 'title' in df.columns:
            df['ticket_id'] = df.apply(
                lambda row: row['ticket_id'] if pd.notna(row['ticket_id']) else (
                    re.search(r'#(\d+)', row['title']).group(1)
                    if isinstance(row['title'], str) and re.search(r'#(\d+)', row['title'])
                    else None
                ),
                axis=1
            )

        # user -> user_id
        if 'user' in df.columns:
            df['user_id'] = df['user'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)

        # converter metadata e object para JSON
        if 'metadata' in df.columns:
            df['metadata'] = df['metadata'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else None)
        if 'object' in df.columns:
            df['object'] = df['object'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else None)

        # --- Criando colunas extras de data/hora (se houver) ---
        df['created_at_data'] = df['created_at'].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None
        )
        df['created_at_hora'] = df['created_at'].apply(
            lambda x: x.strftime("%H:%M:%S") if pd.notnull(x) else None
        )
        df['updated_at_data'] = df['updated_at'].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notnull(x) else None
        )
        df['updated_at_hora'] = df['updated_at'].apply(
            lambda x: x.strftime("%H:%M:%S") if pd.notnull(x) else None
        )

        # Ordenar colunas (incluir as novas colunas de data/hora)
        colunas_banco = [
            "id", "title", "verb", "user_id", "actor_id", "actor_name",
            "created_at", "updated_at", "created_at_data", "created_at_hora",
            "updated_at_data", "updated_at_hora",
            "object", "user", "ticket_id", "ticket_type", "action", 
            "activity_url", "comment", "subject", "público"
        ]
        for col in colunas_banco:
            if col not in df.columns:
                df[col] = None

        df_final = df[colunas_banco]
        return df_final

    except Exception as e:
        print(f'Erro ao tratar dados: {e}')
        return pd.DataFrame()

def inserir_dados_no_banco(df, batch_size=1000):
    """
    Insere o DataFrame (df) na tabela BD_AtividadesSAC (em batches de 1000).
    """
    try:
        conn = pyodbc.connect(
            f"DRIVER={db_config['driver']};SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};UID={db_config['uid']};PWD={db_config['pwd']}"
        )
        cursor = conn.cursor()

        # Adicionamos as 4 novas colunas aqui também
        colunas_validas = [
            "id", "actor_id", "actor_name", "created_at", "updated_at",
            "created_at_data", "created_at_hora", "updated_at_data", "updated_at_hora",
            "title", "verb", "user_id", "ticket_id", "ticket_type",
            "action", "activity_url", "comment", "subject", "público",
            "object", "user"
        ]

        df = df[[c for c in colunas_validas if c in df.columns]]
        for col in colunas_validas:
            if col not in df.columns:
                df[col] = None

        df = df[colunas_validas]

        colunas = ', '.join([f'[{col}]' for col in df.columns])
        placeholders = ', '.join(['?'] * len(df.columns))
        sql = f"INSERT INTO BD_AtividadesSAC ({colunas}) VALUES ({placeholders})"

        # Pegar índice da coluna "created_at" para exibir em caso de erro
        # (em vez de df.columns.index(...) usamos .get_loc(...))
        index_created_at = df.columns.get_loc("created_at")

        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start + batch_size]
            valores = [
                tuple(
                    json.dumps(valor) if isinstance(valor, (dict, list)) else
                    (None if pd.isna(valor) or valor in [np.nan, "nan", "None", ""] else str(valor))
                    for valor in row
                )
                for _, row in batch.iterrows()
            ]

            for valor in valores:
                try:
                    cursor.execute(sql, valor)
                except pyodbc.Error as e:
                    # Em caso de erro, exibe o valor do created_at
                    print(f'Erro ao inserir linha no banco: {e} - Data: {valor[index_created_at]}')
                    continue

            print(f"Inserindo {len(batch)} registros no banco...")

        conn.commit()
        cursor.close()
        conn.close()
        print("Inserção concluída com sucesso!")
    except pyodbc.Error as e:
        print(f'Erro ao inserir dados no banco: {e}')

def excluir_registros_duplicados():
    try:
        conn = pyodbc.connect(
            f"DRIVER={db_config['driver']};SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};UID={db_config['uid']};PWD={db_config['pwd']}"
        )
        cursor = conn.cursor()

        sql = """
        WITH CTE AS (
            SELECT 
                id, user_id, actor_id, ticket_id, action,
                ROW_NUMBER() OVER (PARTITION BY id, user_id, actor_id, ticket_id, action 
                                   ORDER BY id) AS row_num
            FROM BD_AtividadesSAC
        )
        DELETE FROM CTE WHERE row_num > 1;
        """
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        print("Registros duplicados excluídos com sucesso!")
    except pyodbc.Error as e:
        print(f'Erro ao excluir registros duplicados: {e}')

def exportar_para_excel(df):
    """
    Exporta o DataFrame para Excel. Usamos data/hora atual para nome do arquivo.
    """
    try:
        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"atividades_{now_str}.xlsx"
        df.to_excel(file_name, index=False)
        print(f"Dados exportados para o arquivo {file_name} com sucesso!")
    except Exception as e:
        print(f'Erro ao exportar dados para Excel: {e}')

def executar_extracao(exportar_para_banco=True):
    """
    Puxa TODAS as atividades (máximo 30 dias, pois é a limitação do endpoint),
    trata e insere no banco OU exporta para Excel.
    """
    try:
        # Buscar atividades (todas as páginas)
        atividades_data = buscar_atividades()
        if not atividades_data:
            print("Nenhuma atividade retornada.")
            return

        # Tratar dados
        df = tratar_dados(atividades_data)

        # Inserir ou exportar
        if exportar_para_banco:
            inserir_dados_no_banco(df)
            print("Processo concluído com sucesso! 🚀")
        else:
            exportar_para_excel(df)
            print("Exportação concluída com sucesso! 🚀")

    except Exception as e:
        print(f'Erro ao executar a extração: {e}')

def menu():
    try:
        print("1. Inserir (dos últimos 30 dias) no banco")
        print("2. Exportar (dos últimos 30 dias) para Excel")
        opcao = '1'
        #opcao = input("Digite a opção desejada: ").strip()

        if opcao == '1':
            executar_extracao(exportar_para_banco=True)
        elif opcao == '2':
            executar_extracao(exportar_para_banco=False)
        else:
            print("Opção inválida, encerrando.")
            return

        excluir_registros_duplicados()

    except Exception as e:
        print(f'Erro no menu: {e}')

if __name__ == "__main__":
    menu()

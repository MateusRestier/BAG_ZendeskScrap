import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime, timedelta
import time
import re
import pyodbc
import json  # Import necessário para converter dicionários em strings JSON
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
import os  # Import os

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

# Mapeamento dos campos personalizados
custom_field_ids = {
    '20481751634964': 'Área Retorno',
    '23450471389460': 'Data de Envio Área Responsável',
    '23450335909780': 'Previsão de Retorno Área Responsável',
    '7896616478612': 'Assunto do Email',
    '360041469032': 'Canal de Entrada',
    '360041468692': 'Dúvida',
    '360041432051': 'Solicitação',
    '360041431951': 'Problema',
    '360041432091': 'Outros',
    '22541325': 'Transportadora',
    '8225162131348': 'Produto',
    '360041040172': 'Número do Pedido',
    '360030577731': 'SKU dos Produtos',
    '360040274491': 'Número da NF',
    '23507539076884': 'Estorno: Valor',
    '23465090667540': 'Tipo de Estorno',
    '24157626991892': 'Atendente',
    '360030496932': 'Nome Titular do Pedido',
    '23555735385236': 'Estorno: Causa Raiz',
    '23555716189844': 'Estorno: Tipo de Problema',
    '25219880343316': 'Estorno: Tipo de Pagamento',
    '27112346684948': 'Status da Coleta',
    '25783014985492': 'CD: Troca e Acionamento de Garantia',
    '27112338364436': 'Coleta Solicitada Mais de uma Vez?',
    '27265259806228': 'Caso 100% Resolvido no Atendimento Anterior?',
    '26678660208916': 'Número da Loja',
    '25907732988436': 'CD: Outras Demandas',
    '27112064079636': 'Réplica?',
    '28405635340308': 'Sentimento',
    '26241507056916': 'Status de Assistência Técnica',
    '26256563363348': 'Plano de Ação OS Vencidas',
    '26241374621588': 'Prazo 1ª Cobrança',
    '25808063108756': 'CD: Devolução e Voucher',
    '27112048306068': 'Loja Física ou Loja Virtual',
    '25780172368020': 'Etapas de Coleta',
    '27112103294868': 'Avaliação no RA?',
    '27112199178132': 'Nota da Avaliação',
    '25820195084948': 'Demanda',
    '26256620215444': 'Plano de Ação Insatisfação Resultado de OS',
    '25966692319380': 'Número da OS',
    '27265194513556': 'Cliente Reincidente?',
    '25427606175380': 'Número da NFD',
    '22333255': 'Atribuido_Para'
}

# Lista para armazenar os tickets
tickets_data = []

# Função para buscar tickets de um único dia
def buscar_tickets_por_dia(start_date, end_date):
    query = f'type:ticket created_at>="{start_date}" created_at<"{end_date}"'
    url = f'https://bagaggio.zendesk.com/api/v2/search.json?query={query}'
    page_count = 1
    tickets_data = []  # Agora é uma variável local

    while url:
        try:
            print(f'Buscando dados de {start_date} até {end_date} - Página {page_count}...')
            response = requests.get(url, auth=auth)

            if response.status_code != 200:
                print(f'Erro ao buscar a página {page_count}: {response.status_code}')
                print(f'Mensagem da API: {response.text}')
                break

            data = response.json()
            tickets = data.get('results', [])

            print(f'Total de tickets nesta página: {len(tickets)}')

            for ticket in tickets:
                custom_fields_data = {custom_field_ids.get(str(field['id']), str(field['id'])): field.get('value') 
                                      for field in ticket.get('custom_fields', []) 
                                      if str(field['id']) in custom_field_ids}
                ticket.update(custom_fields_data)
                tickets_data.append(ticket)

            print(f'Total de tickets acumulados até agora: {len(tickets_data)}')

            url = data.get('next_page')
            page_count += 1
            time.sleep(1)  # Pausa para evitar sobrecarga da API
        except requests.RequestException as e:
            print(f'Erro ao fazer a requisição: {e}')
            break
        print(f"Tipo retornado por buscar_tickets_por_dia ({start_date} até {end_date}): {type(tickets_data)}")

    return tickets_data

# Função para tratar dados com Pandas
def tratar_dados(tickets_data):
    try:
        df = pd.DataFrame(tickets_data)

        # Remover colunas indesejadas
        colunas_para_remover = ['custom_fields', 'fields', 'followup_ids', 'due_at', 'collaborator_ids', 'follower_ids', 'email_cc_ids', 'forum_topic_id', 'problem_id']
        df = df.drop(columns=[col for col in colunas_para_remover if col in df.columns], errors='ignore')

        # Subdivisão da coluna 'via'
        def extrair_via_info(via):
            if isinstance(via, dict):
                return {
                    'via_channel': via.get('channel'),
                    'via_from_name': via.get('source', {}).get('from', {}).get('name'),
                    'via_from_address': via.get('source', {}).get('from', {}).get('address'),
                    'via_from_ticket_id': via.get('source', {}).get('from', {}).get('ticket_id'),
                    'via_from_subject': via.get('source', {}).get('from', {}).get('subject'),
                    'via_to_name': via.get('source', {}).get('to', {}).get('name'),
                    'via_to_address': via.get('source', {}).get('to', {}).get('address'),
                    'via_rel': via.get('source', {}).get('rel')
                }
            return {}

        # Aplicar a função de extração da coluna 'via'
        via_info_df = df['via'].apply(extrair_via_info).apply(pd.Series)
        df = pd.concat([df, via_info_df], axis=1)

        # Subdivisão da coluna 'satisfaction_rating'
        def extrair_satisfaction_info(satisfaction):
            if isinstance(satisfaction, dict):
                return {
                    'satisfaction_score': satisfaction.get('score'),
                    'satisfaction_comment': satisfaction.get('comment'),
                    'satisfaction_reason': satisfaction.get('reason'),
                    'satisfaction_reason_id': satisfaction.get('reason_id'),
                    'satisfaction_id': str(satisfaction.get('id'))  # Convertendo para string
                }
            return {}

        # Aplicar a função de extração da coluna 'satisfaction_rating'
        satisfaction_info_df = df['satisfaction_rating'].apply(extrair_satisfaction_info).apply(pd.Series)
        df = pd.concat([df, satisfaction_info_df], axis=1)

        # Conversão de colunas de data para datetime (removendo o fuso horário)
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce').dt.tz_localize(None)
        df['updated_at'] = pd.to_datetime(df['updated_at'], errors='coerce').dt.tz_localize(None)

        # Função para remover colchetes e converter listas em strings
        def tratar_valor(valor):
            if isinstance(valor, list):
                return ', '.join(map(str, valor))  # Converte lista em string separada por vírgula
            elif isinstance(valor, str):
                return re.sub(r'[\[\]]', '', valor)  # Remove colchetes de strings
            return valor

        # Aplicar a função em todas as colunas
        for col in df.columns:
            df[col] = df[col].apply(tratar_valor)

        return df
    except Exception as e:
        print(f'Erro ao tratar dados: {e}')
        return pd.DataFrame()

# Função para remover registros duplicados
def remover_duplicados():
    try:
        conn_str = (
            f"DRIVER={db_config['DRIVER']};"
            f"SERVER={db_config['SERVER']};"
            f"DATABASE={db_config['DATABASE']};"
            f"UID={db_config['UID']};"
            f"PWD={db_config['PWD']}"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        sql = """
        WITH CTE AS (
            SELECT 
                id, 
                created_at, 
                ROW_NUMBER() OVER (PARTITION BY id, created_at ORDER BY id) AS row_num
            FROM BD_TicketsSAC
        )
        DELETE FROM CTE WHERE row_num > 1;
        """
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        print('Registros duplicados removidos com sucesso!')
    except pyodbc.Error as e:
        print(f'Erro ao remover registros duplicados: {e}')

# Configuração do banco de dados
db_config = {
    'DRIVER': 'ODBC Driver 17 for SQL Server',
    'SERVER': f"{os.getenv('DB_SERVER_EXCEL')},{os.getenv('DB_PORT_EXCEL')}",
    'DATABASE': os.getenv('DB_DATABASE_EXCEL'),
    'UID': os.getenv('DB_USER_EXCEL'),
    'PWD': os.getenv('DB_PASSWORD_EXCEL')
}


# Mapeamento das colunas do DataFrame para as colunas do banco de dados
column_mapping = {
    'url': 'url',
    'id': 'id',
    'external_id': 'external_id',
    'via': 'via',
    'created_at': 'created_at',
    'updated_at': 'updated_at',
    'generated_timestamp': 'generated_timestamp',
    'type': 'type',
    'subject': 'subject',
    'raw_subject': 'raw_subject',
    'description': 'description',
    'priority': 'priority',
    'status': 'status',
    'recipient': 'recipient',
    'requester_id': 'requester_id',
    'submitter_id': 'submitter_id',
    'assignee_id': 'assignee_id',
    'organization_id': 'organization_id',
    'group_id': 'group_id',
    'has_incidents': 'has_incidents',
    'is_public': 'is_public',
    'tags': 'tags',
    'satisfaction_rating': 'satisfaction_rating',
    'sharing_agreement_ids': 'sharing_agreement_ids',
    'custom_status_id': 'custom_status_id',
    'encoded_id': 'encoded_id',
    'ticket_form_id': 'ticket_form_id',
    'brand_id': 'brand_id',
    'allow_channelback': 'allow_channelback',
    'allow_attachments': 'allow_attachments',
    'from_messaging_channel': 'from_messaging_channel',
    'result_type': 'result_type',
    # Colunas personalizadas
    'Área Retorno': 'Area_Retorno',
    'Data de Envio Área Responsável': 'Data_de_Envio_Area_Responsavel',
    'Previsão de Retorno Área Responsável': 'Previsao_de_Retorno_Area_Responsável',
    'Assunto do Email': 'Assunto_do_Email',
    'Canal de Entrada': 'Canal_de_Entrada',
    'Dúvida': 'Duvida',
    'Solicitação': 'Solicitacao',
    'Problema': 'Problema',
    'Outros': 'Outros',
    'Transportadora': 'Transportadora',
    'Produto': 'Produto',
    'Número do Pedido': 'Numero_do_Pedido',
    'SKU dos Produtos': 'SKU_dos_Produtos',
    'Número da NF': 'Numero_da_NF',
    'Estorno: Valor': 'Estorno_Valor',
    'Tipo de Estorno': 'Tipo_de_Estorno',
    'Atendente': 'Atendente',
    'Nome Titular do Pedido': 'Nome_Titular_do_Pedido',
    'Estorno: Causa Raiz': 'Estorno_Causa_Raiz',
    'Estorno: Tipo de Problema': 'Estorno_Tipo_de_Problema',
    'Estorno: Tipo de Pagamento': 'Estorno_Tipo_de_Pagamento',
    'Número da Loja': 'Numero_da_Loja',
    'Número da NFD': 'Numero_da_NFD',
    'Etapas de Coleta': 'Etapas_de_Coleta',
    'CD: Troca e Acionamento de Garantia': 'CD_Troca_e_Acionamento_de_Garantia',
    'CD: Devolução e Voucher': 'CD_Devolucao_e_Voucher',
    'Demanda': 'Demanda',
    'CD: Outras Demandas': 'CD_Outras_Demandas',
    'Número da OS': 'Numero_da_OS',
    'Prazo 1ª Cobrança': 'Prazo_1_Cobranca',
    'Status de Assistência Técnica': 'Status_de_Assistencia_Tecnica',
    'Plano de Ação OS Vencidas': 'Plano_de_Acao_OS_Vencidas',
    'Plano de Ação Insatisfação Resultado de OS': 'Plano_de_Acao_Insatisfacao_Resultado_de_OS',
    'Loja Física ou Loja Virtual': 'Loja_Fisica_ou_Loja_Virtual',
    'Réplica?': 'Replica',
    'Avaliação no RA?': 'Avaliacao_no_RA',
    'Nota da Avaliação': 'Nota_da_Avaliacao',
    'Coleta Solicitada Mais de uma Vez?': 'Coleta_Solicitada_Mais_de_uma_Vez',
    'Status da Coleta': 'Status_da_Coleta',
    'Cliente Reincidente?': 'Cliente_Reincidente',
    'Caso 100% Resolvido no Atendimento Anterior?': 'Caso_Resolvido_no_Atendimento_Anterior',
    'Sentimento': 'Sentimento',
    # Colunas subdivididas
    'via_channel': 'via_channel',
    'via_from_name': 'via_from_name',
    'via_from_address': 'via_from_address',
    'via_from_ticket_id': 'via_from_ticket_id',
    'via_from_subject': 'via_from_subject',
    'via_to_name': 'via_to_name',
    'via_to_address': 'via_to_address',
    'via_rel': 'via_rel',
    'satisfaction_score': 'satisfaction_score',
    'satisfaction_comment': 'satisfaction_comment',
    'satisfaction_reason': 'satisfaction_reason',
    'satisfaction_reason_id': 'satisfaction_reason_id',
    'satisfaction_id': 'satisfaction_id',
    'Atribuido_Para': 'Atribuido_Para' 
}

# Função para inserir dados no banco de dados em batches
def inserir_dados_no_banco(df, batch_size=1000):
    try:
        conn_str = (
            f"DRIVER={db_config['DRIVER']};"
            f"SERVER={db_config['SERVER']};"
            f"DATABASE={db_config['DATABASE']};"
            f"UID={db_config['UID']};"
            f"PWD={db_config['PWD']}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Filtra apenas as colunas mapeadas e que existem no DataFrame
        colunas_validas = [col for col in column_mapping.keys() if col in df.columns]
        
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start + batch_size]
            for _, row in batch.iterrows():
                # Prepara os dados para inserção
                data = {}
                for col in colunas_validas:
                    valor = row[col]
                    
                    # Converter dicionários em strings JSON
                    if isinstance(valor, dict):
                        valor = json.dumps(valor)
                    
                    # Tratar valores nulos
                    if pd.isnull(valor):
                        valor = None
                    
                    data[col] = valor

                placeholders = ', '.join(['?'] * len(data))
                columns = ', '.join([column_mapping[col] for col in data.keys()])
                
                # Nome correto da tabela
                sql = f"INSERT INTO BD_TicketsSAC ({columns}) VALUES ({placeholders})"

                try:
                    cursor.execute(sql, list(data.values()))
                except pyodbc.Error as e:
                    print(f"Erro ao inserir o ticket ID {row.get('id', 'desconhecido')}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
    except pyodbc.Error as e:
        print(f'Erro ao inserir dados no banco: {e}')

# Função principal para executar a extração de dados
def executar_extracao(start_date, end_date, exportar_para_banco):
    try:
        print(f'Iniciando a extração de tickets de {start_date} até {end_date}...')

        # Remover registros duplicados antes de iniciar a extração
        remover_duplicados()

        # Loop para buscar dia por dia
        while start_date < end_date:
            next_day = start_date + timedelta(days=1)
            tickets_data = buscar_tickets_por_dia(start_date.strftime('%Y-%m-%d'), next_day.strftime('%Y-%m-%d'))

            # Processar e inserir dados ao fim de cada dia
            if tickets_data:
                df = tratar_dados(tickets_data)
                if exportar_para_banco:
                    print(f'Inserindo dados no banco de dados para o dia {start_date.strftime("%Y-%m-%d")}...')
                    inserir_dados_no_banco(df)
                else:
                    print(f'Exportando dados para o arquivo tickets_zendesk_{start_date.strftime("%Y-%m-%d")}.xlsx...')
                    df.to_excel(f'tickets_zendesk_{start_date.strftime("%Y-%m-%d")}.xlsx', index=False)

            start_date = next_day

        # Remover registros duplicados após a inserção
        remover_duplicados()

        print('Processo concluído com sucesso! 🚀')
    except Exception as e:
        print(f'Erro ao executar a extração: {e}')

def buscar_primeiro_ticket():
    url = 'https://bagaggio.zendesk.com/api/v2/incremental/tickets.json?start_time=0'
    
    try:
        print("Buscando o primeiro ticket registrado...")
        response = requests.get(url, auth=auth)
        
        if response.status_code != 200:
            print(f'Erro ao buscar o primeiro ticket: {response.status_code}')
            print(f'Mensagem da API: {response.text}')
            return None

        data = response.json()
        tickets = data.get('tickets', [])

        if tickets:
            primeiro_ticket = tickets[0]
            created_at = primeiro_ticket.get('created_at')
            print(f"Primeiro ticket encontrado:")
            print(f"ID: {primeiro_ticket.get('id')}")
            print(f"Data de Criação: {created_at}")
            return created_at
        else:
            print("Nenhum ticket encontrado.")
            return None

    except requests.RequestException as e:
        print(f'Erro na requisição: {e}')
        return None


# Função para executar a extração em paralelo
def executar_extracao_paralelo(start_date, end_date, exportar_para_banco):
    try:
        num_workers = max(1, multiprocessing.cpu_count() - 1)
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            date_ranges = []
            current_date = start_date
            while current_date < end_date:
                next_day = current_date + timedelta(days=1)
                date_ranges.append((current_date.strftime('%Y-%m-%d'), next_day.strftime('%Y-%m-%d')))
                current_date = next_day

            # Submete tarefas e mapeia para suas datas
            future_to_date = {
                executor.submit(buscar_tickets_por_dia, start, end): (start, end)
                for start, end in date_ranges
            }

            for future in as_completed(future_to_date):
                start, end = future_to_date[future]
                try:
                    tickets_data = future.result()
                    print(f"Tipo retornado por buscar_tickets_por_dia ({start} até {end}): {type(tickets_data)}")

                    if tickets_data:
                        df = tratar_dados(tickets_data)

                        if exportar_para_banco:
                            print(f'Inserindo dados no banco de dados para o dia {start}...')
                            inserir_dados_no_banco(df)
                        else:
                            print(f'Exportando dados para o arquivo tickets_zendesk_{start}.xlsx...')
                            df.to_excel(f'tickets_zendesk_{start}.xlsx', index=False)

                except Exception as e:
                    print(f"❌ Erro ao processar dados de {start} a {end}: {e}")

        remover_duplicados()
        print('Processo concluído com sucesso! 🚀')

    except Exception as e:
        print(f'Erro ao executar a extração em paralelo: {e}')


def menu():
    try:
        print("1. Rodar o código para D-1")
        print("2. Rodar para os últimos 5 dias")
        print("3. Rodar para um dia específico")
        print("4. Rodar para um intervalo de datas")
        print("5. Descobrir o primeiro ticket registrado")  # Nova opção
        print("6. Exportar para Excel")  # Nova opção
        
        opcao = '2'
        #opcao = input("Digite o número da opção desejada: ")
        
        if opcao == '1':
            start_date = datetime.now() - timedelta(days=1)
            end_date = datetime.now()
            executar_extracao_paralelo(start_date, end_date, exportar_para_banco=True)
        elif opcao == '2':
            start_date = datetime.now() - timedelta(days=5)
            end_date = datetime.now()
            executar_extracao_paralelo(start_date, end_date, exportar_para_banco=True)
        elif opcao == '3':
            start_date_input = input("Digite a data específica (YYYY-MM-DD): ")
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d')
            end_date = start_date + timedelta(days=1)
            executar_extracao_paralelo(start_date, end_date, exportar_para_banco=True)
        elif opcao == '4':
            start_date_input = input("Digite a data de início (YYYY-MM-DD): ")
            end_date_input = input("Digite a data de fim (YYYY-MM-DD): ")
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d') + timedelta(days=1)
            executar_extracao_paralelo(start_date, end_date, exportar_para_banco=True)
        elif opcao == '5':
            buscar_primeiro_ticket()  # Chama a nova função
        elif opcao == '6':
            start_date_input = input("Digite a data de início (YYYY-MM-DD): ")
            end_date_input = input("Digite a data de fim (YYYY-MM-DD): ")
            start_date = datetime.strptime(start_date_input, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_input, '%Y-%m-%d') + timedelta(days=1)
            executar_extracao_paralelo(start_date, end_date, exportar_para_banco=False)
        else:
            print("Opção inválida!")

    except Exception as e:
        print(f'Erro no menu: {e}')

if __name__ == "__main__":
    menu()
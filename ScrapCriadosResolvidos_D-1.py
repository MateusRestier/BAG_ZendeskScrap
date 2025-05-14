import os
import time
import subprocess
import pyodbc
import concurrent.futures
from selenium import webdriver
from datetime import datetime
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains

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

############################################################
#               FUNÇÕES DE TRATAMENTO DE DADOS            #
############################################################

def converter_data(valor, formato="%Y-%m-%dT%H:%M:%S"):
    if pd.isnull(valor):
        return None

    valor_str = str(valor).strip()
    if not valor_str:
        return None

    try:
        dt = datetime.strptime(valor_str, formato)
        if dt.year < 1753 or dt.year > 9999:
            return None
        return dt
    except ValueError:
        return None
    
def tratar_dados_created(df):
    mapping = {
        "ID do ticket": "id_ticket",
        "Status do ticket": "status_ticket",
        "Nome do atribuído": "nome_atribuido",
        "Canal do ticket": "canal_ticket",
        "Canal de Entrada": "canal_entrada",
        "Área retorno": "area_retorno",
        "Função do solicitante": "funcao_solicitante",
        "Função do emissor": "funcao_emissor",
        "Criação do ticket - Carimbo de data/hora": "data_criacao",
        "Resolução do ticket - Carimbo de data/hora": "data_resolucao",
        "Problema": "problema",
        "Dúvida": "duvida",
        "Solicitação": "solicitacao",
        "Outros": "outros",
        "E-mail do solicitante": "email_solicitante",
        "E-mail do emissor": "email_emissor",
        "Nome da organização do ticket": "org_ticket",
        "Nome da organização do solicitante": "org_solicitante",
        "Marca do ticket": "marca_ticket",
        "Formulário de ticket": "formulario_ticket"
    }

    df = df.rename(columns=mapping)
    df = df[list(mapping.values())]

    for col in ["data_criacao", "data_resolucao"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: converter_data(x, "%Y-%m-%dT%H:%M:%S"))

    # Substituir campos vazios ("", NaN, espaços) por None
    df = df.map(lambda x: None if pd.isna(x) or str(x).strip() == "" else x)

    print("✅ Dados de 'Created Tickets' tratados com sucesso.")
    return df


def tratar_dados_solved(df):
    mapping = {
        "ID do ticket": "id_ticket",
        "Status do ticket": "status_ticket",
        "Nome do atribuído": "nome_atribuido",
        "Criação do ticket - Data": "data_criacao",
        "Resolução do ticket - Data": "data_resolucao",
        "Nome do emissor": "nome_emissor",
        "Nome do solicitante": "nome_solicitante",
        "Função do solicitante": "funcao_solicitante",
        "Nome da organização do ticket": "org_ticket",
        "Nome da organização do solicitante": "org_solicitante",
        "Marca do ticket": "marca_ticket",
        "Canal do ticket": "canal_ticket",
        "Canal de Entrada": "canal_entrada",
        "Formulário de ticket": "formulario_ticket",
        "Função do emissor": "funcao_emissor"
    }

    df = df.rename(columns=mapping)
    df = df[list(mapping.values())]

    for col in ["data_criacao", "data_resolucao"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: converter_data(x, "%Y-%m-%d"))

    # Substituir campos vazios ("", NaN, espaços) por None
    df = df.map(lambda x: None if pd.isna(x) or str(x).strip() == "" else x)

    print("✅ Dados de 'Solved Tickets' tratados com sucesso.")
    return df


def exportar_para_excel(df_created, df_solved, caminho_arquivo="tickets_exportados.xlsx"):
    """
    Exporta dois DataFrames para um arquivo Excel com abas separadas.

    Parâmetros:
    - df_created: DataFrame com dados de created tickets tratados
    - df_solved: DataFrame com dados de solved tickets tratados
    - caminho_arquivo: nome do arquivo de saída (padrão: tickets_exportados.xlsx)

    Retorna:
    - Caminho do arquivo Excel gerado
    """
    try:
        with pd.ExcelWriter(caminho_arquivo, engine='xlsxwriter') as writer:
            df_created.to_excel(writer, sheet_name="Created_Tickets", index=False)
            df_solved.to_excel(writer, sheet_name="Solved_Tickets", index=False)

        print(f"✅ Arquivo Excel exportado com sucesso: {caminho_arquivo}")
        return caminho_arquivo

    except Exception as e:
        print(f"❌ Erro ao exportar para Excel: {e}")
        return None

def apagar_arquivos_dwnld(diretorio_absoluto):
    """
    Apaga todos os arquivos .csv do diretório de download especificado.
    
    Parâmetro:
    - diretorio_absoluto: caminho absoluto da pasta (ex: dwnld_dir)
    """
    if not os.path.exists(diretorio_absoluto):
        print(f"⚠️ Pasta '{diretorio_absoluto}' não encontrada.")
        return

    arquivos = [f for f in os.listdir(diretorio_absoluto) if f.lower().endswith(".csv")]
    if not arquivos:
        print("📁 Nenhum arquivo .csv para remover.")
        return

    for arquivo in arquivos:
        try:
            caminho = os.path.join(diretorio_absoluto, arquivo)
            os.remove(caminho)
            print(f"🗑️ Arquivo removido: {arquivo}")
        except Exception as e:
            print(f"❌ Erro ao remover {arquivo}: {e}")


###########################################################
#              CONFIGURAR BROWSER AUTOMATICAMENTE         #
###########################################################

def configure_browser():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")

    # Define diretório de download
    script_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(script_dir, "DWNLD")
    os.makedirs(download_dir, exist_ok=True)

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    # Verifica versão do Chrome
    try:
        output = subprocess.check_output(
            r'reg query "HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon" /v version',
            shell=True
        ).decode()
        chrome_version = "Desconhecida"
        for line in output.splitlines():
            if "version" in line.lower():
                chrome_version = line.split()[-1]
                break
    except Exception:
        chrome_version = "Desconhecida"

    print(f"🌐 Versão do Chrome instalada: {chrome_version}")

    # Baixa ChromeDriver compatível
    driver_path = ChromeDriverManager().install()
    chromedriver_version = os.path.basename(os.path.dirname(driver_path))
    print(f"🧩 Versão do ChromeDriver utilizada: {chromedriver_version}")

    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    return driver, download_dir


def aguardar_novo_download(diretorio, arquivos_antes, timeout=60):
    """
    Aguarda até que um novo arquivo CSV seja detectado na pasta de download.
    Compara com a lista de arquivos existente ANTES de iniciar o download.
    """
    print("⏳ Aguardando novo arquivo CSV ser baixado...")
    tempo_inicial = time.time()

    while time.time() - tempo_inicial < timeout:
        arquivos_atual = [f for f in os.listdir(diretorio) if f.lower().endswith(".csv")]
        novos_arquivos = list(set(arquivos_atual) - set(arquivos_antes))

        if novos_arquivos:
            print(f"✅ Novo arquivo detectado: {novos_arquivos[0]}")
            return os.path.join(diretorio, novos_arquivos[0])

        time.sleep(2)

    print("⚠️ Tempo limite atingido! Nenhum novo CSV encontrado.")
    return None

###########################################################
#                     CONECTAR AO BANCO                   #
###########################################################

def inserir_chunk_generico(df_chunk, chunk_id, cnxn_str, tabela_destino):
    try:
        conn = pyodbc.connect(cnxn_str)
        cursor = conn.cursor()

        colunas = df_chunk.columns.tolist()
        colunas_sql = ", ".join([f"[{col}]" for col in colunas])
        placeholders = ", ".join(["?" for _ in colunas])
        insert_sql = f"INSERT INTO {tabela_destino} ({colunas_sql}) VALUES ({placeholders})"

        inserted_count = 0
        for _, row in df_chunk.iterrows():
            try:
                valores = [str(v)[:255] if isinstance(v, str) else (None if pd.isna(v) else v) for v in row.tolist()]
                cursor.execute(insert_sql, valores)
                inserted_count += 1
            except Exception as e:
                print(f"[Chunk {chunk_id}] ⚠️ Erro ao inserir linha: {row.to_dict()}")
                print("    > Erro:", e)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"[Chunk {chunk_id}] ✅ Inseridos {inserted_count}/{len(df_chunk)} registros.")
    except Exception as e:
        print(f"[Chunk {chunk_id}] ❌ ERRO FATAL: {e}")

def inserir_dataframe_em_tabela(df, tabela_destino):

    cnxn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER_EXCEL')},{os.getenv('DB_PORT_EXCEL')};"
        f"DATABASE={os.getenv('DB_DATABASE_EXCEL')};"
        f"UID={os.getenv('DB_USER_EXCEL')};"
        f"PWD={os.getenv('DB_PASSWORD_EXCEL')};"
        f"Timeout=60;"
    )

    batch_size = 500
    chunks = [df.iloc[i:i+batch_size] for i in range(0, len(df), batch_size)]

    max_workers = max(os.cpu_count() - 1, 1)
    print(f"🚀 Iniciando inserção em {tabela_destino} com {max_workers} threads...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(inserir_chunk_generico, chunk, idx, cnxn_str, tabela_destino): idx
            for idx, chunk in enumerate(chunks)
        }

        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Erro não tratado no chunk {idx}: {e}")


def remover_duplicatas_banco(tabela, colunas_chave):
    """
    Remove registros duplicados de uma tabela SQL Server, mantendo o primeiro.
    
    Parâmetros:
    - tabela: nome da tabela (ex: "BD_CreatedTicketsSAC")
    - colunas_chave: lista de colunas que formam a chave única
    """
    try:
        cnxn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER_EXCEL')},{os.getenv('DB_PORT_EXCEL')};"
        f"DATABASE={os.getenv('DB_DATABASE_EXCEL')};"
        f"UID={os.getenv('DB_USER_EXCEL')};"
        f"PWD={os.getenv('DB_PASSWORD_EXCEL')};"
        f"Timeout=60;"
        )
        conn = pyodbc.connect(cnxn_str)
        cursor = conn.cursor()

        chave = ", ".join(colunas_chave)
        delete_sql = f"""
            WITH CTE AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY {chave}
                           ORDER BY id_ticket -- ou outra coluna identificadora
                       ) AS rn
                FROM {tabela}
            )
            DELETE FROM CTE WHERE rn > 1;
        """

        print(f"🔄 Removendo duplicatas da tabela {tabela}...")
        cursor.execute(delete_sql)
        conn.commit()
        print(f"✅ Duplicatas removidas da tabela {tabela} com base nas colunas: {chave}.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erro ao remover duplicatas da tabela {tabela}: {e}")


###########################################################
#                       LOGIN ZENDESK                     #
###########################################################

def login(driver):
    """
    Realiza login na nova URL do dashboard da Zendesk.
    """
    url_login = "https://bagaggio.zendesk.com/explore/dashboard/6983FA0B966E9A19DDCC31139F34CADDEFF7B09ADB00D1A687A53FCA7BE6DBE7"
    print("🔄 Acessando o site...")
    driver.get(url_login)
    time.sleep(3)

    try:
        email_input = driver.find_element(By.ID, "user_email")
        email_input.send_keys(os.getenv('ZENDESK_EMAIL'))

        senha_input = driver.find_element(By.ID, "user_password")
        senha_input.send_keys(os.getenv('ZENDESK_PASSWORD'))

        botao_entrar = driver.find_element(By.ID, "sign-in-submit-button")
        botao_entrar.click()

        time.sleep(5)

        if "dashboard" in driver.current_url:
            print("✅ Login realizado com sucesso!")
            return True
        else:
            print("⚠️ Erro ao fazer login!")
            return False

    except Exception as e:
        print("⚠️ Erro no processo de login:", e)
        return False

###########################################################
#                   FILTRAR DATA                          #
###########################################################

def filtrar_por_data_ultima_semana(driver):
    try:
        print("🔄 Aguardando o botão 'Tempo' estar visível...")
        botao_tempo = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeTimeFilterWidget-2"))
        )
        botao_tempo.click()

        botao_simples = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeSwitch-1"))
        )
        botao_simples.click()

        ultima_semana = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Última semana')]"))
        )
        ultima_semana.click()

        botao_tempo_close = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeTimeFilterWidget-2"))
        )
        botao_tempo_close.click()

        print("✅ Filtro de data (última semana) aplicado com sucesso!")

    except Exception as e:
        print("⚠️ Erro ao aplicar o filtro de data 'Última semana':", e)

def filtrar_por_data_ontem(driver):
    try:
        print("🔄 Aguardando o botão 'Tempo' estar visível...")
        botao_tempo = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeTimeFilterWidget-2"))
        )
        botao_tempo.click()

        botao_simples = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeSwitch-1"))
        )
        botao_simples.click()

        opcao_ontem = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Ontem')]"))
        )
        opcao_ontem.click()

        botao_tempo_close = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeTimeFilterWidget-2"))
        )
        botao_tempo_close.click()

        print("✅ Filtro de data (ontem) aplicado com sucesso!")

    except Exception as e:
        print("⚠️ Erro ao aplicar o filtro de data 'Ontem':", e)

###########################################################
#                     Created Tickets                     #
###########################################################

def baixar_created_tickets(driver):
    """
    Clica na métrica 'Created tickets' (query ID: 205693081),
    em seguida clica em 'Detalhar', aguarda o carregamento e exporta os dados.
    """
    try:
        # Etapa 1: clicar na métrica
        print("🔎 Procurando a métrica 'Created tickets'...")
        elemento = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "kpi-queryid-205693081"))
        )
        print("👆 Clicando na métrica 'Created tickets'...")
        ActionChains(driver).move_to_element(elemento).pause(1).click().perform()
        print("✅ Clique na métrica realizado com sucesso!")
        time.sleep(3)

        # Etapa 2: clicar no botão 'Detalhar'
        print("🔍 Aguardando botão 'Detalhar' aparecer...")
        botao_detalhar = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[contains(@class, 'drill-in')]/span[contains(text(), 'Detalhar')]")
            )
        )
        print("🔄 Clicando no botão 'Detalhar'...")
        botao_detalhar.click()
        print("✅ Botão 'Detalhar' clicado com sucesso!")
        time.sleep(5)

        # Etapa 3: aguardar 30 segundos para garantir o carregamento
        print("⏳ Aguardando 30 segundos para carregamento completo da tabela...")
        time.sleep(30)

        # Etapa 4: clicar no botão 'Exportar'
        print("⬇️ Procurando botão 'Exportar' para iniciar download...")
        botao_exportar = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-test-id='drill-in-modal-export-button']")
            )
        )
        print("🔄 Clicando no botão 'Exportar'...")
        botao_exportar.click()
        print("✅ Exportação iniciada!")

    except Exception as e:
        print("❌ Erro no processo de exportação de 'Created tickets':", e)

###########################################################
#                     Solved Tickets                      #
###########################################################


def baixar_solved_tickets(driver):
    """
    Clica na métrica 'Solved tickets' (query ID: 205693101),
    em seguida clica em 'Detalhar', aguarda o carregamento e exporta os dados.
    """
    try:
        # Etapa 1: clicar na métrica
        print("🔎 Procurando a métrica 'Solved tickets'...")
        elemento = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "kpi-queryid-205693101"))
        )
        print("👆 Clicando na métrica 'Solved tickets'...")
        ActionChains(driver).move_to_element(elemento).pause(1).click().perform()
        print("✅ Clique na métrica realizado com sucesso!")
        time.sleep(3)

        # Etapa 2: clicar no botão 'Detalhar'
        print("🔍 Aguardando botão 'Detalhar' aparecer...")
        botao_detalhar = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[contains(@class, 'drill-in')]/span[contains(text(), 'Detalhar')]")
            )
        )
        print("🔄 Clicando no botão 'Detalhar'...")
        botao_detalhar.click()
        print("✅ Botão 'Detalhar' clicado com sucesso!")
        time.sleep(5)

        # Etapa 3: aguardar 30 segundos para garantir o carregamento
        print("⏳ Aguardando 30 segundos para carregamento completo da tabela...")
        time.sleep(30)

        # Etapa 4: clicar no botão 'Exportar'
        print("⬇️ Procurando botão 'Exportar' para iniciar download...")
        botao_exportar = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-test-id='drill-in-modal-export-button']")
            )
        )
        print("🔄 Clicando no botão 'Exportar'...")
        botao_exportar.click()
        print("✅ Exportação iniciada!")

    except Exception as e:
        print("❌ Erro no processo de exportação de 'Solved tickets':", e)


###########################################################
#                     EXECUÇÃO PRINCIPAL                  #
###########################################################

if __name__ == "__main__":
    opcao_scraping = "ontem"
    driver, dwnld_dir = configure_browser()

    if login(driver):
        if opcao_scraping == "ontem":
            filtrar_por_data_ontem(driver)
        elif opcao_scraping == "ultima_semana":
            filtrar_por_data_ultima_semana(driver)
        else:
            print("⚠️ Opção de filtro inválida.")
        
        # ===== 1º DOWNLOAD: Created Tickets =====
        arquivos_antes_1 = [f for f in os.listdir(dwnld_dir) if f.lower().endswith(".csv")]
        baixar_created_tickets(driver)
        aguardar_novo_download(dwnld_dir, arquivos_antes_1)

        # ===== 2º DOWNLOAD: Solved Tickets =====
        arquivos_antes_2 = [f for f in os.listdir(dwnld_dir) if f.lower().endswith(".csv")]
        baixar_solved_tickets(driver)
        aguardar_novo_download(dwnld_dir, arquivos_antes_2)

    else:
        print("❌ Falha no login.")
        driver.quit()
        exit()

    driver.quit()

    # Localizar os arquivos com base nas palavras-chave
    arquivos_csv = os.listdir(dwnld_dir)
    arquivo_created = next((f for f in arquivos_csv if "created" in f.lower() and f.endswith(".csv")), None)
    arquivo_solved = next((f for f in arquivos_csv if "solved" in f.lower() and f.endswith(".csv")), None)

    if arquivo_created and arquivo_solved:
        caminho_created = os.path.join(dwnld_dir, arquivo_created)
        caminho_solved = os.path.join(dwnld_dir, arquivo_solved)

        #acao = input("Escolha o que deseja fazer com os dados:\n1 - Exportar para Excel\n2 - Inserir no banco de dados\n>> ")
        acao = '2'

        if acao == "1":
            df_created = pd.read_csv(caminho_created, sep=";", encoding="utf-8-sig")
            df_solved = pd.read_csv(caminho_solved, sep=";", encoding="utf-8-sig")

            df_created_tratado = tratar_dados_created(df_created)
            df_solved_tratado = tratar_dados_solved(df_solved)

            exportar_para_excel(df_created_tratado, df_solved_tratado, "tickets_exportados.xlsx")

        elif acao == "2":
            df_created = pd.read_csv(caminho_created, sep=";", encoding="utf-8-sig")
            df_solved = pd.read_csv(caminho_solved, sep=";", encoding="utf-8-sig")

            df_created_tratado = tratar_dados_created(df_created)
            df_solved_tratado = tratar_dados_solved(df_solved)

            inserir_dataframe_em_tabela(df_created_tratado, "BD_CreatedTicketsSAC")
            inserir_dataframe_em_tabela(df_solved_tratado, "BD_SolvedTicketsSAC")

            remover_duplicatas_banco(
                "BD_CreatedTicketsSAC",
                [
                    "id_ticket", "status_ticket", "nome_atribuido", "canal_ticket",
                    "canal_entrada", "area_retorno", "funcao_solicitante", "funcao_emissor",
                    "data_criacao", "data_resolucao", "problema", "duvida", "solicitacao",
                    "outros", "email_solicitante", "email_emissor", "org_ticket",
                    "org_solicitante", "marca_ticket", "formulario_ticket"
                ]
            )
            remover_duplicatas_banco(
                "BD_SolvedTicketsSAC",
                [
                    "id_ticket", "status_ticket", "nome_atribuido", "data_criacao",
                    "data_resolucao", "nome_emissor", "nome_solicitante", "funcao_solicitante",
                    "org_ticket", "org_solicitante", "marca_ticket", "canal_ticket",
                    "canal_entrada", "formulario_ticket", "funcao_emissor"
                ]
            )

        else:
            print("❌ Opção inválida.")
        
        apagar_arquivos_dwnld(dwnld_dir)
        
    else:
        print("⚠️ Não foi possível localizar os dois arquivos (created e solved) na pasta de download.")
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import subprocess
import pandas as pd
import pyodbc
import concurrent.futures
from datetime import datetime
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

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

def converter_data(valor):
    """
    Tenta converter o valor (string) no formato 'YYYY-MM-DDTHH:MM:SS' para datetime.
    Se estiver fora do range permitido pelo DATETIME do SQL (1753-01-01 a 9999-12-31),
    ou se houver erro de parsing, retorna None (inserido como NULL).
    """
    if pd.isnull(valor):
        return None
    
    valor_str = str(valor).strip()
    if not valor_str:
        return None
    
    try:
        dt = datetime.strptime(valor_str, "%Y-%m-%dT%H:%M:%S")
        # Verifica se dt está no range do SQL DATETIME
        if dt.year < 1753 or dt.year > 9999:
            return None
        return dt
    except ValueError:
        return None


def tratar_dados(df):
    """
    Faz todos os tratamentos necessários no DataFrame:
      1) Renomeia colunas conforme mapping.
      2) Filtra as colunas essenciais.
      3) Converte Data_Atualizacao (se existir).
      4) Substitui valores vazios em TODAS as colunas por NULL.
    Retorna o DataFrame já tratado para inserção.
    """
    # Mapeamento dos nomes do arquivo -> nomes que iremos usar no DF
    mapping = {
        "ID do ticket da atualização": "ID",
        "Atualização - Carimbo de data/hora": "Data_Atualizacao",
        "Grupo do ticket na atualização": "Grupo",
        "Nome do atualizador": "Nome_Atualizador",
        "Atribuído do ticket na atualização": "Atribuicao_Ticket",
        "Status do ticket na atualização":  "status",
        "Canal da atualização": "canal",
        "Assunto do ticket": "assunto",
        "Tipo de comentário": "tipo_comentario"
    }
    df = df.rename(columns=mapping)

    # Filtra somente as colunas que de fato precisamos inserir
    colunas_desejadas = ["ID", "Data_Atualizacao", "Grupo", "Nome_Atualizador", "Atribuicao_Ticket", "status", "canal", "assunto", "tipo_comentario"]
    colunas_existem = [c for c in colunas_desejadas if c in df.columns]
    df = df[colunas_existem]

    # Converte data/hora se existir
    if "Data_Atualizacao" in df.columns:
        df["Data_Atualizacao"] = df["Data_Atualizacao"].apply(converter_data)

    # Substitui valores vazios ("", NaN, pd.NA) por None em TODAS as colunas
    df = df.map(lambda x: None if pd.isna(x) or str(x).strip() == "" else x)

    print("✅ Dados tratados com sucesso! Todas as colunas vazias foram convertidas para NULL.")

    return df


############################################################
#            FUNÇÃO PARA INSERIR UM LOTE (BATCH)           #
############################################################

def inserir_chunk(df_chunk, chunk_id, cnxn_str):
    """
    Recebe um DataFrame (df_chunk), o índice do chunk (chunk_id),
    e a string de conexão (cnxn_str).
    Faz a conexão com o banco, insere cada linha em dbo.BD_TicketsAtribuicao.
    Se uma linha der erro, pula só aquela linha.
    Gera logs de sucesso/erro.
    """
    try:
        conn = pyodbc.connect(cnxn_str)
        cursor = conn.cursor()
        cursor.fast_executemany = True

        insert_sql = """
            INSERT INTO dbo.BD_TicketsAtribuicaoSAC
            (ID, Data_Atualizacao, Grupo, Nome_Atualizador, Atribuicao_Ticket, status, canal, assunto, tipo_comentario)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        inserted_count = 0
        total_lines = len(df_chunk)

        for idx, row in df_chunk.iterrows():
            try:
                cursor.execute(
                    insert_sql,
                    row.get("ID"),
                    row.get("Data_Atualizacao"),
                    row.get("Grupo"),
                    row.get("Nome_Atualizador"),
                    row.get("Atribuicao_Ticket"),
                    row.get("status"),
                    row.get("canal"),
                    row.get("assunto"),
                    row.get("tipo_comentario")
                )
                inserted_count += 1
            except Exception as e:
                print(f"[Chunk {chunk_id}] ERRO ao inserir linha: {row.to_dict()}")
                print("    > Erro:", e)
                # Apenas pula a linha que deu erro

        conn.commit()
        cursor.close()
        conn.close()

        print(f"[Chunk {chunk_id}] Finalizado! Inseridos {inserted_count} de {total_lines} linhas.")

    except Exception as e:
        print(f"[Chunk {chunk_id}] ERRO FATAL: {e}")

def inserir_dados(filepath):
    """
    Lê o arquivo (XLSX/XLS/CSV), chama a função de tratamento,
    divide em batches de 500 linhas e insere em paralelo no banco.
    """
    # 1) Ler o arquivo com pandas
    if filepath.lower().endswith(".csv"):
        # Ajuste o 'sep' e o 'encoding' conforme seu CSV
        df = pd.read_csv(filepath, sep=';', encoding='utf-8-sig')
    else:
        df = pd.read_excel(filepath)

    print(">>> Arquivo sendo processado:", filepath)
    print(">>> Colunas detectadas antes do tratamento:", df.columns.tolist())

    # 2) Tratar os dados
    df_tratado = tratar_dados(df)

    print(">>> Colunas finais após tratamento:", df_tratado.columns.tolist())
    print(">>> Registros a inserir:", len(df_tratado))

    # 3) Converte o DataFrame em chunks de 500 linhas
    batch_size = 500
    chunks = []
    for start in range(0, len(df_tratado), batch_size):
        end = start + batch_size
        chunk_df = df_tratado.iloc[start:end]
        chunks.append(chunk_df)

    # 4) String de conexão com SQL Server
    cnxn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER_EXCEL')},{os.getenv('DB_PORT_EXCEL')};"
        f"DATABASE={os.getenv('DB_DATABASE_EXCEL')};"
        f"UID={os.getenv('DB_USER_EXCEL')};"
        f"PWD={os.getenv('DB_PASSWORD_EXCEL')};"
        f"Timeout=60;"
    )

    # 5) Paralelismo: cria um ThreadPoolExecutor com (nucleos - 1) threads
    max_workers = max(os.cpu_count() - 1, 1)
    print(f">>> Iniciando inserções em paralelo (max_workers={max_workers})...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for chunk_id, df_chunk in enumerate(chunks):
            future = executor.submit(inserir_chunk, df_chunk, chunk_id, cnxn_str)
            futures[future] = chunk_id

        for future in concurrent.futures.as_completed(futures):
            chunk_id = futures[future]
            try:
                future.result()
                print(f">>> Chunk {chunk_id} concluído sem exceções.")
            except Exception as e:
                print(f">>> Chunk {chunk_id} gerou exceção não tratada: {e}")
    
    # 6) Após a conclusão da inserção, deletar o arquivo
    if os.path.exists(filepath):
        print(f"🗑️ Deletando arquivo: {filepath}")
        os.remove(filepath)
        print("✅ Arquivo deletado com sucesso.")
    else:
        print("⚠️ Arquivo não encontrado para deleção.")

    # 7) Remover duplicatas no banco
    remover_duplicatas_banco()

    print(">>> FIM do processamento do arquivo:", filepath)


def remover_duplicatas_banco():
    """
    Remove registros duplicados na tabela BD_TicketsAtribuicaoSAC.
    A chave única para remoção é baseada na concatenação de:
    ID + Data_Atualizacao + Nome_Atualizador + Atribuicao_Ticket + status + canal.
    Apenas uma ocorrência de cada combinação será mantida.
    """
    try:
        # Configuração da conexão com o SQL Server
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
        cursor.fast_executemany = True

        print("🔄 Removendo registros duplicados...")

        # SQL para remover duplicatas, garantindo que apenas uma ocorrência seja mantida
        delete_sql = """
            WITH CTE AS (
            SELECT
                ID,
                Data_Atualizacao,
                Nome_Atualizador,
                Atribuicao_Ticket,
                status,
                canal,
                ROW_NUMBER() OVER (PARTITION BY ID,
                Data_Atualizacao,
                Nome_Atualizador,
                Atribuicao_Ticket,
                status,
                canal
            ORDER BY
                ID) AS row_num
            FROM
                BD_TicketsAtribuicaoSAC
                    )
                    DELETE
            FROM
                CTE
            WHERE
                row_num > 1;
        """

        cursor.execute(delete_sql)
        rows_deleted = cursor.rowcount  # Obtém o número de registros removidos

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Remoção de duplicatas concluída! {rows_deleted} registros duplicados foram excluídos.")

    except Exception as e:
        print(f"⚠️ Erro ao remover duplicatas: {e}")


###########################################################
#                   CONFIGURAÇÃO SELENIUM                #
###########################################################

def configure_browser():
    """
    Configura o navegador Chrome com diretório de download e retorna o driver.
    Utiliza o ChromeDriverManager para garantir compatibilidade com o navegador instalado.
    """
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-gpu")

    # Define diretório de download para os arquivos exportados
    script_dir = os.path.dirname(os.path.abspath(__file__))
    download_dir = os.path.join(script_dir, "DWNLD")
    os.makedirs(download_dir, exist_ok=True)

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    # Obtem a versão do Chrome instalada
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

    # Instala e utiliza o ChromeDriver compatível
    driver_path = ChromeDriverManager().install()
    chromedriver_version = os.path.basename(os.path.dirname(driver_path))
    print(f"🧩 Versão do ChromeDriver utilizada: {chromedriver_version}")

    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    return driver, download_dir

###########################################################
#                     FUNÇÃO LOGIN                        #
###########################################################

def login(driver):
    """
    Acessa a página de login do Zendesk, insere as credenciais e faz login.
    """
    url_login = "https://bagaggio.zendesk.com/explore/dashboard/58607DCDDC833A13BAC85055929A451A84C1AA411A997070F5CC00974813E3A6/tab/38874001"

    print("🔄 Acessando o site...")
    driver.get(url_login)
    time.sleep(3)

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

###########################################################
#                   FILTRAR DATA                          #
###########################################################

def filtrar_por_data_ultima_semana(driver):
    """
    Clica no botão de 'Tempo', depois em 'Simples', e seleciona 'Última semana'.
    Em seguida, fecha a janela do filtro para permitir outros cliques,
    utilizando WebDriverWait para garantir que cada elemento esteja clicável.
    """
    try:
        print("🔄 Aguardando o botão 'Tempo' estar visível...")
        botao_tempo = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeTimeFilterWidget-2"))
        )
        print("🔄 Clicando no botão 'Tempo'...")
        botao_tempo.click()

        print("🔄 Aguardando a opção 'Simples' estar visível...")
        botao_simples = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeSwitch-1"))
        )
        print("🔄 Clicando em 'Simples'...")
        botao_simples.click()

        print("🔄 Aguardando a opção 'Última semana' estar visível...")
        ultima_semana = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Última semana')]"))
        )
        print("🔄 Selecionando 'Última semana'...")
        ultima_semana.click()

        print("🔄 Aguardando o botão 'Tempo' (para fechar) estar visível...")
        botao_tempo_close = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeTimeFilterWidget-2"))
        )
        print("🔄 Fechando o menu do filtro...")
        botao_tempo_close.click()

        print("✅ Filtro de data (última semana) aplicado com sucesso!")

    except Exception as e:
        print("⚠️ Erro ao aplicar o filtro de data 'Última semana':", e)

def filtrar_por_data_ontem(driver):
    """
    Clica no botão de 'Tempo', depois em 'Simples', e seleciona 'Ontem'.
    Em seguida, fecha a janela do filtro para permitir outros cliques,
    utilizando WebDriverWait para garantir que cada elemento esteja clicável.
    """
    try:
        print("🔄 Aguardando o botão 'Tempo' estar visível...")
        botao_tempo = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeTimeFilterWidget-2"))
        )
        print("🔄 Clicando no botão 'Tempo'...")
        botao_tempo.click()

        print("🔄 Aguardando a opção 'Simples' estar visível...")
        botao_simples = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeSwitch-1"))
        )
        print("🔄 Clicando em 'Simples'...")
        botao_simples.click()

        print("🔄 Aguardando a opção 'Ontem' estar visível...")
        opcao_ontem = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(text(), 'Ontem')]"))
        )
        print("🔄 Selecionando 'Ontem'...")
        opcao_ontem.click()

        print("🔄 Aguardando o botão 'Tempo' (para fechar) estar visível...")
        botao_tempo_close = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.ID, "bimeTimeFilterWidget-2"))
        )
        print("🔄 Fechando o menu do filtro...")
        botao_tempo_close.click()

        print("✅ Filtro de data (ontem) aplicado com sucesso!")

    except Exception as e:
        print("⚠️ Erro ao aplicar o filtro de data 'Ontem':", e)

###########################################################
#               FAZER DOWNLOAD DO CSV                    #
###########################################################

def baixar_csv(driver):
    """
    Clica especificamente no número 'Agent updates' (kpi-queryid-199487651),
    depois no botão 'Detalhar', seleciona colunas e exporta o CSV.
    """
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dwnld_dir = os.path.join(script_dir, "DWNLD")

        # Aguarda a página carregar
        time.sleep(5)

        # **PASSO 1: Localizar o número da métrica "Agent updates" (kpi-queryid-199487651)**
        print("🔄 Localizando o número da métrica (Agent updates)...")
        numero_xpath = "//div[contains(@class,'kpi-first-measure-value') and contains(@class,'kpi-queryid-199487651')]"
        numero_elemento = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, numero_xpath))
        )

        # Clique mais "realista": move até o elemento, faz pausa e clica
        ActionChains(driver).move_to_element(numero_elemento).pause(1).click().perform()
        print("✅ Número 'Agent updates' clicado!")

        time.sleep(3)  # Espera o botão "Detalhar" aparecer

        # **PASSO 2: Clicar no botão "Detalhar"**
        print("🔄 Procurando botão 'Detalhar'...")
        botao_detalhar = driver.find_element(
            By.XPATH,
            "//div[contains(@class, 'drill-in')]/span[contains(text(), 'Detalhar')]"
        )
        botao_detalhar.click()
        print("✅ Clique em 'Detalhar' realizado!")

        time.sleep(5)  # Aguarda carregamento da tela de detalhamento

        # **PASSO 3: Aguardar a seta para baixo estar visível**
        print("🔄 Aguardando a seta de seleção de colunas aparecer...")
        seta_xpath = "//div[contains(@class, 'StyledTextFauxInput')]"
        botao_seta = WebDriverWait(driver, 60).until(
            EC.element_to_be_clickable((By.XPATH, seta_xpath))
        )

        # **PASSO 4: Clicar na seta para abrir o menu de colunas**
        print("🔄 Clicando na seta para abrir o menu de colunas...")
        botao_seta.click()
        time.sleep(2)
        print("✅ Menu de colunas aberto com sucesso!")

        # **PASSO 5: Selecionar colunas desejadas**
        colunas_xpath = [
            "//li[@id='downshift-1-item-6']",  # Status do ticket na atualização
            "//li[@id='downshift-1-item-5']",  # Canal da atualização
            "//li[@id='downshift-1-item-7']",  # Atribuído do ticket na atualização
            "//li[@id='downshift-1-item-10']",  # Assunto do ticket
        ]

        for xpath in colunas_xpath:
            try:
                coluna = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                coluna.click()
                print(f"✅ Coluna selecionada: {coluna.text}")
                time.sleep(1)
            except Exception as e:
                print(f"⚠️ Erro ao selecionar coluna {xpath}: {e}")

        print("✅ Todas as colunas foram selecionadas!")

        # **PASSO 6: Aguardar o botão 'Exportar' ficar clicável**
        print("🔄 Aguardando o botão 'Exportar' aparecer e ficar clicável...")
        botao_exportar_xpath = "//button[@data-test-id='drill-in-modal-export-button']"
        botao_exportar = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, botao_exportar_xpath))
        )

        # **PASSO 7 : Clicar no botão 'Exportar' para fechar o menu de colunas e Aguardar 30 segundos antes de clicar no botão (ou ajuste se quiser menos)****
        print("🔄 Clicando no botão 'Exportar' para fechar o menu de colunas ...")
        botao_exportar.click() 
        print("⏳ Esperando 30 segundos depois de clicar no botão 'Exportar' para garantir que a tabela carregou ...")
        time.sleep(30)

        # **PASSO 8: Clicar no botão "Exportar"**
        print("🔄 Clicando no botão 'Exportar' para fazer o download ...")
        botao_exportar.click()
        print("✅ Exportação iniciada!")

        # **PASSO 9: Aguardar o download ser concluído**
        aguardar_download(dwnld_dir)

    except Exception as e:
        print("⚠️ Erro ao abrir o menu de colunas, selecionar colunas ou exportar:", e)


def aguardar_download(dwnld_dir, timeout=60):
    """
    Aguarda até que um arquivo CSV seja detectado na pasta de downloads.
    """
    print("⏳ Aguardando o download do arquivo CSV ser concluído...")
    tempo_inicial = time.time()

    while time.time() - tempo_inicial < timeout:
        arquivos = [f for f in os.listdir(dwnld_dir) if f.endswith(".csv")]
        if arquivos:
            print(f"✅ Download concluído! Arquivo encontrado: {arquivos[0]}")
            return arquivos[0]  # Retorna o nome do arquivo baixado
        time.sleep(2)  # Aguarda 2 segundos antes de verificar novamente

    print("⚠️ Tempo limite atingido! Nenhum arquivo CSV encontrado.")
    return None

###########################################################
#                     EXECUÇÃO PRINCIPAL                  #
###########################################################

if __name__ == "__main__":
    executar_scraping = True
    executar_processamento = True

    # Escolha entre "ontem" ou "ultima_semana"
    opcao_scraping = "ontem"

    driver, dwnld_dir = configure_browser()

    if executar_scraping:
        if login(driver):
            if opcao_scraping == "ontem":
                # Filtrar ontem e baixar
                filtrar_por_data_ontem(driver)
                baixar_csv(driver)

                # Fechando navegador
                driver.quit()


            elif opcao_scraping == "ultima_semana":
                # Filtrar última semana e baixar
                filtrar_por_data_ultima_semana(driver)
                baixar_csv(driver)

                # Fechando navegador
                driver.quit()

            else:
                print("⚠️ Opção de scraping inválida. Nenhum download será realizado.")
        else:
            print("⚠️ Falha no login. A extração não será realizada.")

    if executar_processamento:
        # Em vez de aguardar 1 download específico, iremos processar TODOS os .csv que já estiverem na pasta
        csv_encontrados = [f for f in os.listdir(dwnld_dir) if f.lower().endswith(".csv")]

        if not csv_encontrados:
            print("⚠️ Nenhum arquivo CSV encontrado para processar!")
        else:
            print(f"📂 Arquivos detectados: {csv_encontrados}. Iniciando processamento...")

            for nome_arquivo_csv in csv_encontrados:
                caminho_arquivo = os.path.join(dwnld_dir, nome_arquivo_csv)
                print(f"🔄 Processando arquivo: {caminho_arquivo}")

                # 1) Tratar e inserir no banco
                inserir_dados(caminho_arquivo)

            # 3) Excluir TODOS os .csv após o processamento
            for f in os.listdir(dwnld_dir):
                if f.lower().endswith(".csv"):
                    os.remove(os.path.join(dwnld_dir, f))
            print("🗑️ Todos os arquivos .csv foram removidos.")

    print("🏁 Fim da execução.")
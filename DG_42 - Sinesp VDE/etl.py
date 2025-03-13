import pandas as pd
import os
import glob
import sqlalchemy as sa
from loguru import logger as log
from log import log_decorator
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from config import CONFIG


@log_decorator
def verificar_e_baixar_arquivos(pasta_destino: str, url_base: str) -> list:
    """
    Verifica se os arquivos XLSX da página da web existem na pasta de destino (Data).
    Se não existirem, faz o download dos arquivos faltantes.
    """
    log.info(f"Verificando arquivos XLSX na pasta {pasta_destino}")
    
    # Criar a pasta de destino se não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
        log.info(f"Pasta {pasta_destino} criada com sucesso")
    
    # Obter a lista de arquivos existentes na pasta
    arquivos_existentes = set(os.listdir(pasta_destino))
    log.info(f"Encontrados {len(arquivos_existentes)} arquivos na pasta")
    
    # Scrapping da página para encontrar links para arquivos XLSX
    try:
        log.info(f"Acessando a URL: {url_base}")
        response = requests.get(url_base)
        response.raise_for_status()  # Verificar se a requisição foi bem-sucedida
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Encontrar todos os links para arquivos XLSX
        links_xlsx = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.xlsx') and '/download/' in href:
                links_xlsx.append(href)
        
        log.info(f"Encontrados {len(links_xlsx)} arquivos XLSX na página")
        
        # Arquivos baixados
        arquivos_baixados = []
        
        # Verificar quais arquivos precisam ser baixados
        for link in links_xlsx:
            # Extrair o nome do arquivo do link
            nome_arquivo = unquote(os.path.basename(link.split('/download/file')[0]))
            
            # Verificar se o arquivo já existe na pasta
            if nome_arquivo not in arquivos_existentes:
                log.info(f"Arquivo {nome_arquivo} não encontrado localmente. Iniciando download...")
                
                # URL completa para download
                url_download = link
                
                # Garantir que a URL está completa
                if not url_download.startswith('http'):
                    url_download = urljoin(url_base, url_download)
                
                try:
                    # Baixar o arquivo
                    arquivo_response = requests.get(url_download, stream=True)
                    arquivo_response.raise_for_status()
                    
                    # Salvar o arquivo localmente
                    caminho_completo = os.path.join(pasta_destino, nome_arquivo)
                    with open(caminho_completo, 'wb') as arquivo:
                        for chunk in arquivo_response.iter_content(chunk_size=8192):
                            arquivo.write(chunk)
                    
                    log.info(f"Download de {nome_arquivo} concluído com sucesso")
                    arquivos_baixados.append(nome_arquivo)
                    
                except Exception as e:
                    log.error(f"Erro ao baixar o arquivo {nome_arquivo}: {str(e)}")
            else:
                log.info(f"Arquivo {nome_arquivo} já existe localmente")
        
        log.info(f"Verificação e download concluídos. {len(arquivos_baixados)} arquivos foram baixados")
        return arquivos_baixados
        
    except Exception as e:
        log.error(f"Erro ao acessar a página ou analisar o HTML: {str(e)}")
        return []



@log_decorator
def extrair_dados(pasta:str, arquivos_para_processar=None) -> pd.DataFrame:
    """
    Extrai dados de arquivos xlsx específicos ou todos da pasta e retorna um DataFrame
    Adiciona uma coluna com o nome do arquivo para cada registro
    
    Args:
        pasta: Caminho da pasta com os arquivos
        arquivos_para_processar: Lista de nomes de arquivos para processar (opcional)
                                Se None, processa todos os arquivos
    
    Returns:
        DataFrame: Dados combinados com coluna adicional 'nome_arquivo'
    """
    log.info(f"Buscando arquivos na pasta {pasta}")
    
    # Obter todos os arquivos Excel da pasta
    todos_arquivos = glob.glob(os.path.join(pasta, '*.xlsx'))
    
    # Filtrar apenas os arquivos que precisam ser processados, se especificado
    if arquivos_para_processar is not None:
        arquivos = [os.path.join(pasta, arquivo) for arquivo in arquivos_para_processar 
                   if os.path.exists(os.path.join(pasta, arquivo))]
        log.info(f"Processando apenas {len(arquivos)} arquivos específicos")
    else:
        arquivos = todos_arquivos
        log.info(f"Processando todos os {len(arquivos)} arquivos Excel encontrados")
    
    if not arquivos:
        log.warning(f"Nenhum arquivo Excel encontrado para processar em: {pasta}")
        return pd.DataFrame()

    dataframes = []
    for arquivo in arquivos:
        try:
            # Extrair apenas o nome do arquivo sem o caminho
            nome_arquivo = os.path.basename(arquivo)
            
            # Ler o arquivo Excel
            df = pd.read_excel(arquivo)
            
            # Adicionar coluna com o nome do arquivo a cada registro
            df['nome_arquivo'] = nome_arquivo
            
            dataframes.append(df)
            log.info(f"Arquivo {nome_arquivo} processado com sucesso: {len(df)} registros")
            
        except Exception as e:
            log.error(f"Erro ao processar o arquivo {arquivo}: {str(e)}")
    
    if not dataframes:
        log.warning("Nenhum dataframe foi criado. Verifique os erros acima.")
        return pd.DataFrame()

    dados_combinados = pd.concat(dataframes, ignore_index=True)
    log.info(f"Extração de dados realizada com sucesso: {len(dados_combinados)} registros no total")

    return dados_combinados

@log_decorator
def ajustar_colunas(dados:pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta os tipos de colunas e trata valores faltantes
    """
    # Criar uma cópia para não modificar o original
    df_ajustado = dados.copy()
    
    # Converter colunas numéricas
    colunas_numericas = ['feminino', 'masculino', 'nao_informado', 'total_vitima', 'total', 'total_peso']
    for col in colunas_numericas:
        if col in df_ajustado.columns:
            df_ajustado[col] = pd.to_numeric(df_ajustado[col], errors='coerce')
            df_ajustado[col].fillna(0, inplace=True)

    # Para colunas categóricas
    colunas_categoricas = ['uf', 'municipio', 'evento', 'agente', 'arma', 'faixa_etaria']
    for col in colunas_categoricas:
        if col in df_ajustado.columns:
            df_ajustado[col].fillna('Não informado', inplace=True)

    # Remover a coluna 'formulario' se ela existir
    if 'formulario' in df_ajustado.columns:
        df_ajustado = df_ajustado.drop('formulario', axis=1)

    # Muda UFs para maiúsculo
    if 'uf' in df_ajustado.columns:
        df_ajustado['uf'] = df_ajustado['uf'].str.upper()
    
    # nomes dos municípios primeira letra maiúscula
    if 'municipio' in df_ajustado.columns:
        df_ajustado['municipio'] = df_ajustado['municipio'].str.title()

    # Adicionar região e nome do estado - método otimizado
    if 'uf' in df_ajustado.columns:
        # Dicionários para mapeamento
        mapa_regioes = {
            'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
            'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste',
            'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
            'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste',
            'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
            'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
        }
        
        mapa_estados = {
            'AC': 'Acre',
            'AL': 'Alagoas',
            'AP': 'Amapá',
            'AM': 'Amazonas',
            'BA': 'Bahia',
            'CE': 'Ceará',
            'DF': 'Distrito Federal',
            'ES': 'Espírito Santo',
            'GO': 'Goiás',
            'MA': 'Maranhão',
            'MT': 'Mato Grosso',
            'MS': 'Mato Grosso do Sul',
            'MG': 'Minas Gerais',
            'PA': 'Pará',
            'PB': 'Paraíba',
            'PR': 'Paraná',
            'PE': 'Pernambuco',
            'PI': 'Piauí',
            'RJ': 'Rio de Janeiro',
            'RN': 'Rio Grande do Norte',
            'RS': 'Rio Grande do Sul',
            'RO': 'Rondônia',
            'RR': 'Roraima',
            'SC': 'Santa Catarina',
            'SP': 'São Paulo',
            'SE': 'Sergipe',
            'TO': 'Tocantins'
        }
        
        # Usar .map() para aplicar os dicionários - muito mais eficiente que iterrows
        df_ajustado['regiao'] = df_ajustado['uf'].map(mapa_regioes).fillna('Não informado')
        df_ajustado['nome_estado'] = df_ajustado['uf'].map(mapa_estados).fillna('Não informado')
        
        log.info(f"Adicionadas colunas 'regiao' e 'nome_estado' ao DataFrame")
    
    return df_ajustado

@log_decorator
def transformar_datas(dados_planilha:pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a coluna de datas para o formato datetime
    """
    # Criar uma cópia para não modificar o original
    df_prep_data = dados_planilha.copy()

    # Verificar se a coluna existe antes de transformar
    if 'data_referencia' not in df_prep_data.columns:
        log.warning("Coluna 'data_referencia' não encontrada. Pulando transformação de datas.")
        return df_prep_data
    
    # Converter a coluna de data para datetime
    df_prep_data['data_referencia'] = pd.to_datetime(df_prep_data['data_referencia'], errors='coerce')
        
    # Extrair componentes da data para análise temporal
    df_prep_data['ano'] = df_prep_data['data_referencia'].dt.year
    df_prep_data['mes'] = df_prep_data['data_referencia'].dt.month

    log.info(f"Transformação de datas realizada com sucesso")
    return df_prep_data

@log_decorator
def categorizar_eventos(df:pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona uma coluna de categoria aos eventos criminais para facilitar a análise
    """
    # Verificar se a coluna evento existe
    if 'evento' not in df.columns:
        log.warning("Coluna 'evento' não encontrada. Pulando categorização de eventos.")
        return df
        
    # Definição das categorias
    categorias_eventos = {
        'Crimes Violentos Letais': [
            'Homicídio doloso',
            'Feminicídio',
            'Lesão corporal seguida de morte',
            'Roubo seguido de morte (latrocínio)',
            'Morte por intervenção de Agente do Estado'
        ],
        'Crimes Violentos Não Letais': [
            'Tentativa de homicídio',
            'Tentativa de feminicídio',
            'Estupro',
            'Estupro de vulnerável'
        ],
        'Crimes Contra Patrimônio': [
            'Roubo de veículo',
            'Furto de veículo',
            'Roubo a instituição financeira',
            'Roubo de carga'
        ],
        'Tráfico e Apreensões': [
            'Tráfico de drogas',
            'Apreensão de Cocaína',
            'Apreensão de Maconha',
            'Arma de Fogo Apreendida'
        ],
        'Mortes Diversas': [
            'Morte no trânsito ou em decorrência dele (exceto homicídio doloso)',
            'Mortes a esclarecer (sem indício de crime)',
            'Suicídio',
            'Suicídio de Agente do Estado',
            'Morte de Agente do Estado'
        ],
        'Outros': [
            'Pessoa Desaparecida',
            'Pessoa Localizada',
            'Mandado de prisão cumprido',
            'Busca e salvamento',
            'Combate a incêndios',
            'Atendimento pré-hospitalar',
            'Emissão de Alvarás de licença',
            'Realização de vistorias'
        ]
    }
   
    # Função auxiliar para classificar
    def classificar_evento(evento: str) -> str:
        for categoria, eventos in categorias_eventos.items():
            if evento in eventos:
                return categoria
        return 'Não Classificado'
    
    # Cópia para não modificar o original
    df_cat = df.copy()
    
    # Adicionar categoria a cada registro
    df_cat['categoria'] = df_cat['evento'].apply(classificar_evento)
    
    return df_cat


@log_decorator
def criar_agregacoes(df:pd.DataFrame) -> dict:
    """
    Cria agregações úteis para análises
    Retorna um dicionário com diferentes DataFrames agregados
    """
    agregacoes = {}
    
    # Agregação por UF e Evento
    if all(col in df.columns for col in ['uf', 'evento', 'categoria']):
        colunas_agregacao = ['total_vitima', 'feminino', 'masculino', 'total', 'total_peso']
        colunas_disponiveis = [col for col in colunas_agregacao if col in df.columns]
        
        if colunas_disponiveis:
            agg_dict = {col: 'sum' for col in colunas_disponiveis}
            agg_uf_evento = df.groupby(['uf', 'evento', 'categoria']).agg(agg_dict).reset_index()
            agregacoes['uf_evento'] = agg_uf_evento
    
    # Agregação temporal (por mês e ano)
    if all(col in df.columns for col in ['ano', 'mes', 'evento', 'categoria']):
        colunas_agregacao = ['total_vitima', 'feminino', 'masculino', 'total']
        colunas_disponiveis = [col for col in colunas_agregacao if col in df.columns]
        
        if colunas_disponiveis:
            agg_dict = {col: 'sum' for col in colunas_disponiveis}
            agg_temporal = df.groupby(['ano', 'mes', 'evento', 'categoria']).agg(agg_dict).reset_index()
            agregacoes['temporal'] = agg_temporal
    
    # Agregação para foco específico nas categorias solicitadas
    if 'evento' in df.columns:
        categorias_foco = [
            'Feminicídio', 
            'Tentativa de feminicídio',
            'Suicídio',
            'Homicídio doloso',
            'Estupro',
            'Estupro de vulnerável',
            'Roubo de veículo',
            'Furto de veículo'
        ]
        
        df_foco = df[df['evento'].isin(categorias_foco)]
        if not df_foco.empty:
            # Por UF
            if 'uf' in df.columns:
                colunas_agregacao = ['total_vitima', 'feminino', 'masculino', 'total']
                colunas_disponiveis = [col for col in colunas_agregacao if col in df.columns]
                
                if colunas_disponiveis:
                    agg_dict = {col: 'sum' for col in colunas_disponiveis}
                    agg_foco_uf = df_foco.groupby(['uf', 'evento']).agg(agg_dict).reset_index()
                    agregacoes['foco_uf'] = agg_foco_uf
            
            # Temporal
            if all(col in df.columns for col in ['ano', 'mes']):
                colunas_agregacao = ['total_vitima', 'feminino', 'masculino', 'total']
                colunas_disponiveis = [col for col in colunas_agregacao if col in df.columns]
                
                if colunas_disponiveis:
                    agg_dict = {col: 'sum' for col in colunas_disponiveis}
                    agg_foco_temporal = df_foco.groupby(['ano', 'mes', 'evento']).agg(agg_dict).reset_index()
                    agregacoes['foco_temporal'] = agg_foco_temporal
    
    log.info(f"Criadas {len(agregacoes)} agregações para análise")
    return agregacoes


@log_decorator
def salvar_no_banco(df, tabela_nome, engine, if_exists='replace'):
    """
    carrega o DataFrame no banco de dados
    """
    try:
        df.to_sql(tabela_nome, engine, if_exists=if_exists, index=False, 
                 schema='public', chunksize=1000)
        log.info(f"Dados salvos com sucesso na tabela {tabela_nome}")
        return True
    except Exception as e:
        log.error(f"Erro ao salvar dados na tabela {tabela_nome}: {str(e)}")
        return False
    

@log_decorator
def criar_conexao_bd(tipo_bd, usuario, senha, host, porta, nome_bd):
    """
    Cria uma conexão com o banco de dados
    """
    try:
        string_conexao = f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{nome_bd}"   
        engine = sa.create_engine(string_conexao)
        log.info(f"Conexão com banco de dados {tipo_bd} em {host}:{porta} estabelecida com sucesso")
        return engine
    except Exception as e:
        log.error(f"Erro ao conectar ao banco de dados: {str(e)}")
        return None
    


@log_decorator
def obter_arquivos_processados(engine, nome_tabela='dados_seguranca_publica'):
    """
    Consulta o banco de dados para obter a lista de arquivos já processados.
    
    Args:
        engine: Conexão com o banco de dados
        nome_tabela: Nome da tabela onde os arquivos processados estão armazenados
        
    Returns:
        set: Conjunto de nomes de arquivos que já foram processados
    """
    try:
        
        # Verificar se a tabela existe
        inspector = sa.inspect(engine)
        tabelas_existentes = inspector.get_table_names(schema='public')
        
        if nome_tabela not in tabelas_existentes:
            log.info(f"Tabela {nome_tabela} não existe no banco de dados")
            return set()
        
        # Verificar se a coluna nome_arquivo existe
        colunas = inspector.get_columns(nome_tabela)
        colunas_nomes = [col['name'] for col in colunas]
        
        if 'nome_arquivo' not in colunas_nomes:
            log.warning(f"Coluna 'nome_arquivo' não existe na tabela {nome_tabela}")
            return set()
        
        # Consultar os arquivos já processados
        query = f"SELECT DISTINCT nome_arquivo FROM {nome_tabela} WHERE nome_arquivo IS NOT NULL"
        df_arquivos = pd.read_sql_query(query, engine)
        
        arquivos_processados = set(df_arquivos['nome_arquivo'].tolist())
        log.info(f"Encontrados {len(arquivos_processados)} arquivos já processados no banco")
        
        return arquivos_processados
        
    except Exception as e:
        log.error(f"Erro ao consultar arquivos processados: {str(e)}")
        return set()


@log_decorator
def executar_etl(pasta_dados, tipo_bd, usuario, senha, host, porta, nome_bd, url_base=None):
    """
    Executa o pipeline do ETL com verificação de arquivos já processados
    """
    log.info(f" === INICIANDO PROCESSO  ===")
    log.info(f"Origem dos dados: {pasta_dados}")
    
    # Verificar e baixar arquivos se necessário
    arquivos_baixados = []
    if url_base:
        log.info(f"Verificando se todos os arquivos necessários estão disponíveis...")
        arquivos_baixados = verificar_e_baixar_arquivos(pasta_dados, url_base)
        if arquivos_baixados:
            log.info(f"Foram baixados {len(arquivos_baixados)} arquivos: {', '.join(arquivos_baixados)}")
    
    # Obter a lista de todos os arquivos na pasta
    try:
        arquivos_disponiveis = [f for f in os.listdir(pasta_dados) if f.endswith('.xlsx')]
        log.info(f"Encontrados {len(arquivos_disponiveis)} arquivos Excel na pasta")
    except Exception as e:
        log.error(f"Erro ao listar arquivos na pasta {pasta_dados}: {str(e)}")
        return False
    
    if not arquivos_disponiveis:
        log.warning(f"Nenhum arquivo Excel encontrado na pasta {pasta_dados}")
        return False
    
    # Criar conexão com o banco de dados
    engine = criar_conexao_bd(tipo_bd, usuario, senha, host, porta, nome_bd)
    if engine is None:
        log.error("Falha na conexão com o banco de dados. Encerrando processo.")
        return False
    
    # Verificar quais arquivos já foram processados
    arquivos_processados = obter_arquivos_processados(engine)
    
    # Determinar quais arquivos precisam ser processados
    arquivos_para_processar = [arquivo for arquivo in arquivos_disponiveis 
                              if arquivo not in arquivos_processados] 
    
    # Se nenhum arquivo novo para processar, encerrar o ETL
    if not arquivos_para_processar and not arquivos_baixados:
        log.info("Todos os arquivos disponíveis já foram processados. Não há novos dados para inserir.")
        log.info("=== PROCESSO DE ETL CONCLUÍDO SEM ALTERAÇÕES ===")
        return True
    
    log.info(f"Serão processados {len(arquivos_para_processar)} novos arquivos")
    for arquivo in arquivos_para_processar:
        log.info(f"  - {arquivo}")
    
    # EXTRAÇÃO - apenas dos novos arquivos
    dados_brutos = extrair_dados(pasta_dados, arquivos_para_processar)
    if dados_brutos.empty:
        log.error("Nenhum dado extraído dos novos arquivos. Encerrando processo.")
        return False
    
    # TRANSFORMAÇÃO
    log.info(f"Iniciando transformações dos dados ({len(dados_brutos)} registros)...")
    
    # Processamento de datas
    dados_datas = transformar_datas(dados_brutos)
    
    # Ajuste de colunas e tipos de dados
    dados_ajustados = ajustar_colunas(dados_datas)
    
    # Categorização dos eventos
    dados_categorizados = categorizar_eventos(dados_ajustados)
    
    # Dados finais
    dados_finais = dados_categorizados
    
    # CARGA
    log.info(f"Iniciando carga dos dados no banco {tipo_bd}...")
    
    # Verificar se a tabela existe
    
    inspector = sa.inspect(engine)
    tabela_existe = 'dados_seguranca_publica' in inspector.get_table_names()
    
    modo_insercao = 'append' if tabela_existe else 'replace'
    log.info(f"Modo de inserção: {modo_insercao}")
    
    # Salvar dados no banco
    sucesso_carga = salvar_no_banco(dados_finais, 'dados_seguranca_publica', engine, modo_insercao)
    
    # Verificar sucesso da operação
    if sucesso_carga:
        log.info(f"=== PROCESSO DE ETL CONCLUÍDO COM SUCESSO ===")
        log.info(f"Dados de {len(arquivos_para_processar)} novos arquivos inseridos na tabela 'dados_seguranca_publica'")
        
        # Contagem de registros na tabela
        try:
            contagem_sql = "SELECT COUNT(*) FROM dados_seguranca_publica"
            contagem = pd.read_sql(contagem_sql, engine).iloc[0, 0]
            log.info(f"Total de registros na tabela: {contagem}")
        except Exception as e:
            log.warning(f"Não foi possível contar os registros: {str(e)}")
    else:
        log.error(f"=== PROCESSO DE ETL FALHOU ===")
        log.error(f"Não foi possível salvar os novos dados na tabela.")
    
    return sucesso_carga

if __name__ == "__main__":
    import argparse
    
    print("Iniciando ......../n")
    
    sucesso = executar_etl(
        pasta_dados=CONFIG['pasta_dados'],
        tipo_bd=CONFIG['tipo_bd'],
        usuario=CONFIG['usuario'],
        senha=CONFIG['senha'],
        host=CONFIG['host'],
        porta=CONFIG['porta'],
        nome_bd=CONFIG['nome_bd'],
        url_base=CONFIG['url_base']
    )
    
    sys.exit(0 if sucesso else 1)

  
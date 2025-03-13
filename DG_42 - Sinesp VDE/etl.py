import pandas as pd
import os
import glob
import numpy as np
import sqlalchemy
from datetime import datetime
from loguru import logger as log
import sys
from log import log_decorator


# Definição do caminho para os arquivos
caminho_arquivos_xlsx = r'Data'



@log_decorator
def extrair_dados(pasta:str) -> pd.DataFrame:
    """
    Extrai dados de todos os xlsx que estao na pasta e retorna um DataFrame
    """

    log.info(f"buscando arquivos na pasta {pasta}")
    arquivos = glob.glob(os.path.join(pasta, '*.xlsx'))
    
    if not arquivos:
        log.warning(f"Nenhum arquivo Excel encontrado em: {pasta}")
        return pd.DataFrame()

    dataframes = []
    for arquivo in arquivos:
        df = pd.read_excel(arquivo)
        dataframes.append(df)

    dados_combinados = pd.concat(dataframes, ignore_index=True)
    
    log.info(f"Extração de dados realizada com sucesso")

    return dados_combinados

@log_decorator
def transformar_datas(dados_planilha:pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a coluna de datas para o formato datetime
    """

    # famoso copia mas não faz igual.
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
    df_prep_data['trimestre'] = df_prep_data['data_referencia'].dt.quarter
    df_prep_data['semestre'] = ((df_prep_data['mes'] - 1) // 6) + 1

    log.info(f"Transformação de datas realizada com sucesso")
    return df_prep_data

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

    return df_ajustado

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
def criar_metricas_adicionais(df:pd.DataFrame) -> pd.DataFrame:
    """
    Cria métricas adicionais úteis para análise em Power BI
    """
    df_metricas = df.copy()
    
    # Proporção de vitimas por gênero
    colunas_necessarias = ['feminino', 'masculino', 'total_vitima']
    if all(col in df_metricas.columns for col in colunas_necessarias):
        # Evitar divisão por zero
        df_metricas['proporcao_vitimas_femininas'] = np.where(
            df_metricas['total_vitima'] > 0,
            df_metricas['feminino'] / df_metricas['total_vitima'] * 100,
            0
        )
        df_metricas['proporcao_vitimas_masculinas'] = np.where(
            df_metricas['total_vitima'] > 0,
            df_metricas['masculino'] / df_metricas['total_vitima'] * 100,
            0
        )
    else:
        log.warning("Algumas colunas necessárias para cálculo de proporção de vítimas não foram encontradas")

    # Verificar se a coluna evento existe
    if 'evento' in df_metricas.columns:
        # Indicador de severidade para crimes (para facilitar escala de cores no dashboard)
        # 1: Baixa, 2: Média, 3: Alta, 4: Crítica
        def calcular_severidade(evento):
            if evento in ['Homicídio doloso', 'Feminicídio', 'Lesão corporal seguida de morte',
                         'Roubo seguido de morte (latrocínio)', 'Morte por intervenção de Agente do Estado']:
                return 4  # Crítica
            elif evento in ['Tentativa de homicídio', 'Tentativa de feminicídio', 'Estupro', 'Estupro de vulnerável']:
                return 3  # Alta
            elif evento in ['Roubo de veículo', 'Furto de veículo', 'Roubo a instituição financeira', 'Roubo de carga',
                           'Tráfico de drogas']:
                return 2  # Média
            else:
                return 1  # Baixa

        df_metricas['severidade'] = df_metricas['evento'].apply(calcular_severidade)
    else:
        log.warning("Coluna 'evento' não encontrada. Pulando cálculo de severidade.")

    # Criar campos de geolocalização para Power BI
    # Estes serão usados para visualizações geográficas
    if 'uf' in df_metricas.columns:
        df_metricas['regiao'] = df_metricas['uf'].apply(mapear_regiao)
    else:
        log.warning("Coluna 'uf' não encontrada. Pulando mapeamento de região.")

    log.info(f"Métricas adicionais criadas com sucesso")
    return df_metricas

def mapear_regiao(uf):
    """
    Mapeia UF para a respectiva região do Brasil
    """
    mapa_regioes = {
        'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
        'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste',
        'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
        'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste',
        'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
        'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
    }
    return mapa_regioes.get(uf, 'Não informado')


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
    Salva o DataFrame no banco de dados
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
        engine = sqlalchemy.create_engine(string_conexao)
        log.info(f"Conexão com banco de dados {tipo_bd} em {host}:{porta} estabelecida com sucesso")
        return engine
    except Exception as e:
        log.error(f"Erro ao conectar ao banco de dados: {str(e)}")
        return None   


@log_decorator
def executar_etl(pasta_dados, tipo_bd, usuario, senha, host, porta, nome_bd):
    """
    Executa o pipeline completo de ETL
    """
    log.info(f"=== INICIANDO PROCESSO DE ETL ===")
    log.info(f"Origem dos dados: {pasta_dados}")
    
    # EXTRAÇÃO
    dados_brutos = extrair_dados(pasta_dados)
    if dados_brutos.empty:
        log.error("Nenhum dado extraído. Encerrando processo.")
        return False
    
    # TRANSFORMAÇÃO
    log.info(f"Iniciando transformações dos dados...")
    
    # Processamento de datas
    dados_datas = transformar_datas(dados_brutos)
    
    # Ajuste de colunas e tipos de dados
    dados_ajustados = ajustar_colunas(dados_datas)
    
    # Categorização dos eventos
    dados_categorizados = categorizar_eventos(dados_ajustados)
    
    # Cálculo de métricas adicionais
    dados_finais = criar_metricas_adicionais(dados_categorizados)
    
    # Criar agregações para facilitar análises
    agregacoes = criar_agregacoes(dados_finais)
    
    # CARGA
    log.info(f"Iniciando carga dos dados no banco {tipo_bd}...")
    
    # Conectar ao banco de dados
    engine = criar_conexao_bd(tipo_bd, usuario, senha, host, porta, nome_bd)
    if engine is None:
        log.error("Falha na conexão com o banco de dados. Encerrando processo.")
        return False
    
    # Salvar tabela principal
    sucesso_principal = salvar_no_banco(dados_finais, 'fatos_seguranca', engine, 'replace')
    
    # Salvar agregações
    sucessos_agregacoes = []
    for nome, df_agg in agregacoes.items():
        sucesso = salvar_no_banco(df_agg, f'agg_{nome}', engine, 'replace')
        sucessos_agregacoes.append(sucesso)
    
    # Verificar sucesso de todas as operações
    todos_sucessos = sucesso_principal and all(sucessos_agregacoes)
    if todos_sucessos:
        log.info(f"=== PROCESSO DE ETL CONCLUÍDO COM SUCESSO ===")
    else:
        log.warning(f"=== PROCESSO DE ETL CONCLUÍDO COM AVISOS ===")
    
    return todos_sucessos

# Executar o ETL se este arquivo for executado diretamente
if __name__ == "__main__":
    import argparse
    
    # Configurar argumentos da linha de comando
    parser = argparse.ArgumentParser(description='ETL para dados de segurança pública')
    parser.add_argument('--pasta', '-p', type=str, default='Data', 
                        help='Pasta onde estão os arquivos de dados (xlsx)')
    parser.add_argument('--bd', '-b', type=str, default='postgres', choices=['postgres'],
                        help='Tipo de banco de dados')
    parser.add_argument('--usuario', '-u', type=str, default='dgu',
                        help='Usuário do banco de dados')
    parser.add_argument('--senha', '-s', type=str, required=True,
                        help='Senha do banco de dados')
    parser.add_argument('--host', type=str, default='localhost',
                        help='Host do banco de dados')
    parser.add_argument('--porta', type=str, default='5433',
                        help='Porta do banco de dados')
    parser.add_argument('--nome_bd', '-n', type=str, default='data_glow_up',
                        help='Nome do banco de dados')
    
    args = parser.parse_args()
    

    #  Insira os argumentos para rodar a etl.
    # Executar o pipeline de ETL python etl.py -p "Data" -b postgres -u dgu -s 123456 --host localhost --porta 5433 -n data_glow_up
    sucesso = executar_etl(
        pasta_dados=args.pasta,
        tipo_bd=args.bd,
        usuario=args.usuario,
        senha=args.senha,
        host=args.host,
        porta=args.porta,
        nome_bd=args.nome_bd
    )
    
    sys.exit(0 if sucesso else 1)

  
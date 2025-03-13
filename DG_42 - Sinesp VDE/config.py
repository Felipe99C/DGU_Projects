# Configurações fixas para o ETL
CONFIG = {

    #arquivos dos dados
    'pasta_dados': 'Data',
    
    # Configurações do banco de dados como é local mesmo foda-se
    'tipo_bd': 'postgres',
    'usuario': 'dgu',
    'senha': '123456',  
    'host': 'localhost',
    'porta': '5433',
    'nome_bd': 'data_glow_up',
    
    # URL da página do governo com os arquivos de dados 
    'url_base': "https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/estatistica/dados-nacionais-1/base-de-dados-e-notas-metodologicas-dos-gestores-estaduais-sinesp-vde-2022-e-2023"
}
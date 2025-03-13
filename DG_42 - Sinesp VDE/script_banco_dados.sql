--script para criar o banco de dados

CREATE DATABASE dgu_42
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    --LC_COLLATE = 'pt_BR.UTF-8'
    --LC_CTYPE = 'pt_BR.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

COMMENT ON DATABASE dgu_42 IS 'Banco de dados para análise de indicadores de segurança pública';


-- Tabela principal
CREATE TABLE IF NOT EXISTS public.tb_dados_seguranca (
    id SERIAL PRIMARY KEY,
    uf VARCHAR(2),
    municipio VARCHAR(100),
    evento VARCHAR(100),
    data_referencia TIMESTAMP,
    agente VARCHAR(100),
    arma VARCHAR(100),
    faixa_etaria VARCHAR(50),
    feminino NUMERIC,
    masculino NUMERIC,
    nao_informado NUMERIC,
    total_vitima NUMERIC,
    total NUMERIC,
    total_peso NUMERIC,
    abrangencia VARCHAR(50),
    formulario VARCHAR(50),
    ano INTEGER,
    mes INTEGER,
    trimestre INTEGER,
    semestre INTEGER,
    categoria VARCHAR(50),
    proporcao_vitimas_femininas NUMERIC,
    proporcao_vitimas_masculinas NUMERIC,
    severidade INTEGER,
    regiao VARCHAR(30),
    ano_arquivo INTEGER
);

-- Indices
CREATE INDEX idx_fatos_uf ON public.tb_dados_seguranca(uf);
CREATE INDEX idx_fatos_evento ON public.tb_dados_seguranca(evento);
CREATE INDEX idx_fatos_categoria ON public.tb_dados_seguranca(categoria);
CREATE INDEX idx_fatos_data ON public.tb_dados_seguranca(data_referencia);
CREATE INDEX idx_fatos_anomes ON public.tb_dados_seguranca(ano, mes);
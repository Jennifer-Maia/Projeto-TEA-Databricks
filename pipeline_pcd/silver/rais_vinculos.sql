-- 1. Tabela SILVER Principal: Enriquecimento (JOIN) e Conversão de Tipos
CREATE OR REFRESH LIVE TABLE rais_vinculos_enriquecidos
COMMENT "Tabela SILVER da RAIS, limpa, enriquecida e com correção de escala salarial."
TBLPROPERTIES ("quality" = "silver")
AS

WITH silver_limpeza AS (
    SELECT
        T1.*, 
        -- Tratamento de Salário
        REPLACE(REPLACE(T1.valor_remuneracao_media, '.', ''), ',', '.') AS salario_bruto_limpo,
        REPLACE(REPLACE(T1.valor_remuneracao_media_sm, '.', ''), ',', '.') AS salario_sm_limpo
    FROM LIVE.bronze.rais AS T1
)
-- 2. SELECT principal que utiliza a CTE e os JOINS
SELECT
    T1.ano,
    T1.sigla_uf,
    
    -- Conversão de Salário: Divide por 100 para centavos (Ex: R$ 164194 -> R$ 1641.94)
    (CAST(T1.salario_bruto_limpo AS DECIMAL(18, 2)) / 100.0) AS salario_medio_bruto,
    (CAST(T1.salario_sm_limpo AS DECIMAL(18, 2)) / 100.0) AS salario_medio_sm,
    
    T1.idade,
    T1.sexo,
    
    -- Tradução CBO (Ocupação)
    T1.cbo_2002 AS ocupacao_codigo,
    T2.descricao_atividade AS ocupacao_descricao,
    T2.descricao_grande_area AS cbo_grande_area,
    
    -- Tradução de Deficiência
    T1.tipo_deficiencia AS deficiencia_codigo,
    T3.valor AS deficiencia_descricao,
    
    -- Tradução de Escolaridade
    T1.grau_instrucao_apos_2005 AS instrucao_codigo,
    T4.valor AS instrucao_descricao,
    
    T1.indicador_portador_deficiencia,
    T1.vinculo_ativo_3112,
    LOWER(T1.tipo_vinculo) AS tipo_vinculo

FROM silver_limpeza AS T1 -- USA a CTE limpa aqui
    
-- JOINS com Dicionários BRONZE
LEFT JOIN LIVE.bronze.cbo_dicionario AS T2 
    ON T1.cbo_2002 = T2.cbo_2002
    
LEFT JOIN LIVE.bronze.rais_dicionario AS T3 
    ON T1.tipo_deficiencia = T3.chave 
    AND T3.nome_coluna = 'tipo_deficiencia'
    
LEFT JOIN LIVE.bronze.rais_dicionario AS T4 
    ON T1.grau_instrucao_apos_2005 = T4.chave 
    AND T4.nome_coluna = 'grau_instrucao_apos_2005';
CREATE OR REFRESH STREAMING LIVE TABLE rais
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
ano,
cbo_2002,
faixa_etaria,
faixa_remuneracao_media_sm,
grau_instrucao_apos_2005,
idade,
indicador_portador_deficiencia,
nome_deficiencia,
sexo,
sigla_uf,
subatividade_ibge,
subsetor_ibge,
tipo_deficiencia,
tipo_vinculo,
valor_remuneracao_media,
valor_remuneracao_media_sm,
vinculo_ativo_3112
FROM STREAM(lakehouse_tea.raw_ingestion.rais);
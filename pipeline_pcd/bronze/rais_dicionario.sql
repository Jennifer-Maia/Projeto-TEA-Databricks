CREATE OR REFRESH STREAMING LIVE TABLE rais_dicionario
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  chave,
  cobertura_temporal,
  id_tabela,
  nome_coluna,
  valor
FROM STREAM(lakehouse_tea.raw_ingestion.rais_dicionario);
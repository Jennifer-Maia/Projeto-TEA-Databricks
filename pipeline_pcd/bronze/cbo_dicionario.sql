CREATE OR REFRESH STREAMING LIVE TABLE cbo_dicionario
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
  atividade,
  cbo_2002,
  descricao_atividade,
  descricao_grande_area,
  grande_area
FROM STREAM(lakehouse_tea.raw_ingestion.cbo_dicionario);
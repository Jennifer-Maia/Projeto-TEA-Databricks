

CREATE OR REFRESH LIVE TABLE kpis_proporcao_formal_tea
COMMENT "KPIs consolidados para comparação entre Vínculos PCD/TEA e Vínculos Totais Formais, por Ocupação, UF e Escolaridade."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  ano,
  sigla_uf,
  ocupacao_descricao,
  instrucao_descricao,
  
  -- 1. TOTAL DE VÍNCULOS PCD/TEA (Numerador)
  -- Foco na Deficiência Intelectual/Mental (código '4' na RAIS)
  SUM(CASE 
    WHEN deficiencia_codigo = '4' AND indicador_portador_deficiencia = '1'
    THEN 1 ELSE 0 
  END) AS total_vinculos_pcd_tea,
  
  -- 2. TOTAL DE VÍNCULOS FORMAIS (Denominador)
  COUNT(*) AS total_vinculos_geral,
  
  -- 3. PROPORÇÃO (TEA no Total de Vínculos Formais)
  (SUM(CASE 
    WHEN deficiencia_codigo = '4' AND indicador_portador_deficiencia = '1'
    THEN 1 ELSE 0 
  END) / COUNT(*)) AS proporcao_pcd_tea_no_total,
  
  -- 4. MÉDIA SALARIAL (Apenas do grupo PCD/TEA)
  AVG(CASE
    WHEN deficiencia_codigo = '4' AND indicador_portador_deficiencia = '1'
    THEN salario_medio_bruto ELSE NULL
  END) AS media_salarial_pcd_tea
  
FROM LIVE.rais_vinculos_enriquecidos

WHERE ano = '2024'

GROUP BY 1, 2, 3, 4
HAVING total_vinculos_pcd_tea > 0 -- Filtra linhas onde não há ocorrência de TEA
ORDER BY proporcao_pcd_tea_no_total DESC;
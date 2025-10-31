# Databricks notebook source
# MAGIC %sql
# MAGIC    -- 1 Cria o catálogo principal (onde toda a plataforma de dados será organizada),
# MAGIC     CREATE CATALOG IF NOT EXISTS lakehouse_tea
# MAGIC     COMMENT "Catálogo principal do projeto, com camadas de dados governadas via Unity Catalog";
# MAGIC
# MAGIC     -- 2 Cria as quatro camadas da arquitetura de dados
# MAGIC     CREATE SCHEMA IF NOT EXISTS lakehouse_tea.raw
# MAGIC     COMMENT "Camada RAW - dados brutos, como chegam das fontes (SQL, API, planilhas)";
# MAGIC     CREATE VOLUME IF NOT EXISTS lakehouse_tea.raw.rais;
# MAGIC     CREATE VOLUME IF NOT EXISTS lakehouse_tea.raw.rais_dicionario;
# MAGIC     CREATE VOLUME IF NOT EXISTS lakehouse_tea.raw.cbo_dicionario;
# MAGIC
# MAGIC     CREATE SCHEMA IF NOT EXISTS lakehouse_tea.bronze
# MAGIC     COMMENT "Camada BRONZE - dados padronizados, com metadados e controle de ingestão";
# MAGIC
# MAGIC     CREATE SCHEMA IF NOT EXISTS lakehouse_tea.silver
# MAGIC     COMMENT "Camada SILVER - dados tratados, com regras de negócio aplicadas";
# MAGIC
# MAGIC     CREATE SCHEMA IF NOT EXISTS lakehouse_tea.gold
# MAGIC     COMMENT "Camada GOLD - dados analíticos e métricas finais para BI e IA";
# MAGIC
# MAGIC     -- 3 (opcional) Verifica se tudo foi criado corretamente",
# MAGIC     SHOW SCHEMAS IN lakehouse_tea;

# COMMAND ----------

# MAGIC %md
# MAGIC # Criação de Acesso

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2) Permissões (um GRANT por principal)
# MAGIC
# MAGIC -- Catálogo
# MAGIC GRANT USE CATALOG ON CATALOG lakehouse_tea TO data_engineers;
# MAGIC GRANT USE CATALOG ON CATALOG lakehouse_tea TO data_analysts;
# MAGIC GRANT USE CATALOG ON CATALOG lakehouse_tea TO data_scientists;
# MAGIC GRANT USE CATALOG ON CATALOG lakehouse_tea TO business_users;
# MAGIC GRANT USE CATALOG ON CATALOG lakehouse_tea TO analytics_engineers;
# MAGIC
# MAGIC -- RAW (restrito a engenheiros)
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.raw TO data_engineers;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.raw TO data_engineers;
# MAGIC
# MAGIC -- BRONZE (eng: criar/modificar; analistas: leitura)
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.bronze TO data_engineers;
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.bronze TO data_analysts;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.bronze TO data_analysts;
# MAGIC
# MAGIC -- SILVER (leitura p/ times técnicos)
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.silver TO data_engineers;
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.silver TO data_analysts;
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.silver TO data_scientists;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.silver TO data_engineers;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.silver TO data_analysts;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.silver TO data_scientists;
# MAGIC
# MAGIC -- GOLD (leitura ampla, incluindo negócio)
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.gold TO data_engineers;
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.gold TO data_analysts;
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.gold TO data_scientists;
# MAGIC GRANT USE SCHEMA ON SCHEMA lakehouse_tea.gold TO business_users;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.gold TO data_engineers;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.gold TO data_analysts;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.gold TO data_scientists;
# MAGIC -- GRANT SELECT ON ALL TABLES IN SCHEMA lakehouse_tea.gold TO business_users;

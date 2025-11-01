import streamlit as st
import pandas as pd
from databricks import sql
import os
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go

#######################################################################

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# --- 1. CONFIGURAÇÃO E CREDENCIAIS (Lidas do .env) ---
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG_SCHEMA_TABLE = "lakehouse_tea.gold.kpis_proporcao_formal_gold"


#######################################################################

# --- 2. FUNÇÃO DE CONEXÃO E CACHE ---
@st.cache_data(ttl=600)  # Cacheia os dados por 10 minutos para otimizar custos
def load_data():
    """Conecta-se ao Databricks SQL Warehouse e carrega a tabela GOLD."""
    if not all([DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN]):
        st.error("Erro de Configuração: Por favor, preencha o arquivo .env com as credenciais do Databricks.")
        return pd.DataFrame(), 0

    try:
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        ) as connection:
            # Query para agregação nacional e por UF
            query = f"""
            SELECT 
                sigla_uf, 
                SUM(total_vinculos_pcd_tea) AS total_tea,
                SUM(total_vinculos_geral) AS total_geral,
                AVG(media_salarial_pcd_tea) AS media_salarial_tea
            FROM {CATALOG_SCHEMA_TABLE}
            GROUP BY 1
            """
            # Query para Ocupações e Escolaridade
            detail_query = f"""
            SELECT ocupacao_descricao, instrucao_descricao, total_vinculos_pcd_tea
            FROM {CATALOG_SCHEMA_TABLE}
            WHERE total_vinculos_pcd_tea > 0
            ORDER BY total_vinculos_pcd_tea DESC
            LIMIT 10
            """
            
            with connection.cursor() as cursor:
                cursor.execute(query)
                df_uf = cursor.fetchall_pandas()

                cursor.execute(detail_query)
                df_detalhe = cursor.fetchall_pandas()

            # Cálculo da Proporção Geral
            total_vinculos = df_uf['total_geral'].sum()
            total_tea = df_uf['total_tea'].sum()
            proporcao_geral = (total_tea / total_vinculos) if total_vinculos else 0
            
            return df_uf, df_detalhe, proporcao_geral

    except Exception as e:
        st.error(f"Erro ao conectar ou carregar dados do Databricks: {e}")
        st.code(f"Detalhes do Erro: {e}", language='text')
        return pd.DataFrame(), pd.DataFrame(), 0
    
#######################################################################

# --- 3. INTERFACE DO STREAMLIT ---
st.set_page_config(layout="wide", page_title="Inclusão Formal (TEA/PCD Intelectual)")

df_uf, df_detalhe = load_data()

st.title("🧩 Inclusão Formal de PCD Intelectual/TEA no Brasil (RAIS 2024)")
st.caption("Base: RAIS 2024 (Estimativa) - Filtrado para Deficiência Intelectual/Mental (Cód. 4).")
st.markdown("---")

if not df_uf.empty:
    
    # === A. KPIs NACIONAIS ===
    st.header("1. Indicadores Nacionais Consolidados")
    
    col1, col2, col3 = st.columns(3)
    
    # KPI 1: Proporção Nacional
    col1.metric(
        label="Proporção Nacional de Vínculos TEA/PCD",
        value=f"{proporcao_geral_nacional * 100:.4f}%",
        delta="O desafio da Inclusão Formal no Brasil",
        delta_color="off"
    )
    
    # KPI 2: Total de Vínculos
    col2.metric(
        label="Total de Vínculos TEA/PCD (Estimativa)",
        value=f"{df_uf['total_tea'].sum():,.0f}",
    )

    # KPI 3: Média Salarial Nacional
    col3.metric(
        label="Média Salarial TEA/PCD (Nacional)",
        value=f"R$ {df_uf['media_salarial_tea'].mean():,.2f}",
    )
    
    st.markdown("---")

    # === B. ANÁLISE GEOGRÁFICA (Mapa) ===
    st.header("2. Geografia da Inclusão")
    
    col_map, col_list = st.columns([7, 3])

    with col_map:
        # Mapeando o valor da proporção
        df_uf['proporcao'] = (df_uf['total_tea'] / df_uf['total_geral']) * 100

        fig = px.choropleth(
            df_uf,
            geojson="https://raw.githubusercontent.com/codeforamerica/click-that-hood/master/datasets/brazil-states.geojson",
            locations='sigla_uf',
            featureidkey='properties.sigla',
            color='proporcao',
            color_continuous_scale=px.colors.sequential.Plotly3,
            scope="south america", # Para centralizar o Brasil, embora o geojson já faça o fit
            hover_name='sigla_uf',
            hover_data={'total_tea': True, 'proporcao': ':.4f', 'total_geral': False},
            title='Proporção de Vínculos TEA/PCD por Unidade da Federação (%)'
        )
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(height=500, margin={"r":0,"t":50,"l":0,"b":0})
        st.plotly_chart(fig, use_container_width=True)

    with col_list:
        st.subheader("UFs com Maior Proporção")
        # Criando uma lista simples de UFs ordenadas
        df_list = df_uf.sort_values('proporcao', ascending=False)[['sigla_uf', 'proporcao']].head(7)
        st.dataframe(df_list.style.format({'proporcao': "{:.4f}%"}), hide_index=True, use_container_width=True)

    st.markdown("---")

    # === C. ANÁLISE DETALHADA: Ocupações e Escolaridade ===
    st.header("3. Detalhes: Ocupações e Escolaridade")
    st.caption(f"Top 10 Vínculos (TEA/PCD) por Ocupação, refletindo a natureza das descrições do CBO.")

    col_detalhe, col_bar = st.columns([5, 5])
    
    # Tabela de Detalhes
    with col_detalhe:
        st.subheader("Ocupações Mais Comuns (Insight Burocrático)")
        st.dataframe(df_detalhe.style.format({'total_vinculos_pcd_tea': "{:,.0f}"}), hide_index=True, use_container_width=True)
    
    # Gráfico de Barras por Escolaridade (Exemplo simples)
    if not df_detalhe.empty:
        df_escolaridade = df_detalhe.groupby('instrucao_descricao')['total_vinculos_pcd_tea'].sum().reset_index()
        with col_bar:
            st.subheader("Distribuição por Escolaridade")
            fig_bar = px.pie(
                df_escolaridade, 
                values='total_vinculos_pcd_tea', 
                names='instrucao_descricao', 
                title='Vínculos TEA/PCD por Grau de Instrução'
            )
            st.plotly_chart(fig_bar, use_container_width=True)
            
else:
    st.info("Aguardando o carregamento dos dados da sua camada GOLD...")
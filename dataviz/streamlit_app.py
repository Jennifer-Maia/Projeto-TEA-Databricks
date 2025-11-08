# dataviz/streamlit_app.py
import streamlit as st
import pandas as pd
from databricks import sql
import os
from dotenv import load_dotenv
import plotly.express as px

# ======================================================================
# CONFIGURAÇÃO GERAL
# ======================================================================
st.set_page_config(
    page_title="Inclusão Formal TEA | Brasil",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Tema escuro customizado
st.markdown("""
    <style>
        body, [data-testid="stAppViewContainer"] {
            background-color: #0E1117;
            color: #FAFAFA;
        }
        h1, h2, h3, h4 { color: #F1F1F1; }
        hr { border: 1px solid #333; }
        [data-testid="stSidebar"] {
            background-color: #1E222A;
            color: #FAFAFA;
        }
        .stMetric {
            background-color: #1E1E1E;
            border-radius: 10px;
            padding: 12px;
        }
        button { border-radius: 8px !important; }
        a {
            color: #58A6FF !important;
            text-decoration: none !important;
        }
    </style>
""", unsafe_allow_html=True)

# ======================================================================
# SIDEBAR
# ======================================================================
with st.sidebar:
    st.markdown("## 🧩 Projeto: Inclusão Formal TEA no Mercado de Trabalho")
    st.write("Explorando vínculos formais e ocupações com base nos microdados da RAIS 2024.")
    st.markdown("---")
    st.markdown("### 📬 Contato & Repositório")
    st.markdown("[![GitHub](https://img.shields.io/badge/-GitHub-100000?style=flat&logo=github&logoColor=white)](https://github.com/Jennifer-Maia/Projeto-TEA-Databricks)")
    st.markdown("[![LinkedIn](https://img.shields.io/badge/-LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/jennifer-n-maia)")
    st.markdown("---")
    st.caption("Status: 🚀 ETL e Dashboard concluídos.")
    st.caption("Autor: Jennifer Maia — Analytics Engineer")

# ======================================================================
# CONEXÃO E CARGA DE DADOS
# ======================================================================
load_dotenv()
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
CATALOG_SCHEMA_TABLE = "lakehouse_tea.gold.kpis_proporcao_formal_tea"

@st.cache_data(ttl=600)
def load_data():
    """Carrega dados da tabela Gold no Databricks."""
    if not all([DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN]):
        st.error("❌ Configuração incorreta. Preencha o arquivo .env.")
        return pd.DataFrame(), pd.DataFrame(), 0

    try:
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        ) as connection:

            query = f"""
            SELECT 
                sigla_uf, 
                SUM(total_vinculos_pcd_tea) AS total_tea,
                SUM(total_vinculos_geral) AS total_geral,
                AVG(media_salarial_pcd_tea) AS media_salarial_tea
            FROM {CATALOG_SCHEMA_TABLE}
            GROUP BY 1
            """

            detail_query = f"""
            SELECT ocupacao_descricao, instrucao_descricao, total_vinculos_pcd_tea
            FROM {CATALOG_SCHEMA_TABLE}
            WHERE total_vinculos_pcd_tea > 0
            ORDER BY total_vinculos_pcd_tea DESC
            LIMIT 10
            """

            with connection.cursor() as cursor:
                cursor.execute(query)
                df_uf = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])
                cursor.execute(detail_query)
                df_detalhe = pd.DataFrame(cursor.fetchall(), columns=[d[0] for d in cursor.description])

            total_vinculos = df_uf['total_geral'].sum()
            total_tea = df_uf['total_tea'].sum()
            proporcao_geral = (total_tea / total_vinculos) if total_vinculos else 0

            return df_uf, df_detalhe, proporcao_geral

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame(), pd.DataFrame(), 0

# ======================================================================
# INTERFACE PRINCIPAL
# ======================================================================
df_uf, df_detalhe, proporcao_geral = load_data()

st.title("💼 Inclusão Formal de Pessoas Autistas — Brasil (RAIS 2024)")
st.caption("Análise baseada nos microdados RAIS — deficiência intelectual (Cód. 4). *Amostra ilustrativa de dados públicos.*")
st.markdown("---")

if not df_uf.empty:
    # KPIs
    st.subheader("📊 Indicadores Nacionais")
    col1, col2, col3 = st.columns(3)
    col1.metric("Proporção Nacional TEA/PCD", f"{proporcao_geral*100:.2f}%")
    col2.metric("Total de Vínculos TEA (amostra)", f"{df_uf['total_tea'].sum():,.0f}")
    col3.metric("Média Salarial TEA (R$)", f"{df_uf['media_salarial_tea'].mean():,.2f}")
    st.markdown("---")

    # =============================================================
    # DISTRIBUIÇÃO GEOGRÁFICA — TABELA
    # =============================================================
    st.subheader("🌍 Distribuição Geográfica — Inclusão Formal TEA")

    df_uf['proporcao'] = (df_uf['total_tea'] / df_uf['total_geral']) * 100
    df_geo = df_uf.sort_values('proporcao', ascending=False)[['sigla_uf', 'proporcao', 'total_tea']]

    st.dataframe(
        df_geo.rename(columns={
            'sigla_uf': 'UF',
            'proporcao': 'Proporção (%)',
            'total_tea': 'Total TEA'
        }).style.format({'Proporção (%)': '{:.2f}', 'Total TEA': '{:,.0f}'}),
        use_container_width=True,
        hide_index=True
    )

    st.markdown("---")

    # =============================================================
    # OCUPAÇÕES E ESCOLARIDADE
    # =============================================================
    st.subheader("🏢 Ocupações e Nível de Instrução")
    #st.subheader("🏢 Ocupações (Top 10)")

    fig_bar = px.bar(
        df_detalhe.sort_values('total_vinculos_pcd_tea', ascending=True),
        x='total_vinculos_pcd_tea',
        y='ocupacao_descricao',
        orientation='h',
        color='total_vinculos_pcd_tea',
        color_continuous_scale='Blues',
        title='Top 10 Ocupações (vínculos TEA/PCD)'
    )
    fig_bar.update_layout(showlegend=False, height=500)
    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.warning("⏳ Carregando dados... verifique a camada GOLD no Databricks.")

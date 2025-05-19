"""
Fluxo Estrangeiro de Investimentos na B3 - Aplicação Streamlit
Este script cria uma aplicação Streamlit para visualização dos dados de fluxo estrangeiro.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
import os
import subprocess
import locale

# Configurar localização para português
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')
    except:
        pass  # Se falhar, mantém o padrão

# Configurações da página
st.set_page_config(
    page_title="Fluxo Estrangeiro B3",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

def carregar_dados(pasta="Dados", atualizar=False):
    """Carrega os dados processados ou executa a atualização se solicitado"""
    arquivos_necessarios = [
        "fluxo_completo.parquet",
        "fluxo_ano_atual.parquet",
        "fluxo_total.parquet"
    ]
    
    # Verificar se os arquivos existem
    arquivos_ausentes = [f for f in arquivos_necessarios if not os.path.exists(f"{pasta}/{f}")]
    
    # Se arquivos estiverem ausentes ou se a atualização for solicitada, executar os scripts
    if arquivos_ausentes or atualizar:
        with st.spinner("Atualizando dados do mercado..."):
            st.info("Coletando dados da B3 e Yahoo Finance...")
            # Executar o script de coleta de dados
            subprocess.run(["python", "1_coleta_dados.py"], check=True)
            
            st.info("Processando dados coletados...")
            # Executar o script de processamento de dados
            subprocess.run(["python", "2_processa_dados.py"], check=True)
    
    # Carregar os dados processados
    fluxo_completo = pd.read_parquet(f"{pasta}/fluxo_completo.parquet")
    fluxo_ano_atual = pd.read_parquet(f"{pasta}/fluxo_ano_atual.parquet")
    fluxo_total = pd.read_parquet(f"{pasta}/fluxo_total.parquet")
    
    return fluxo_completo, fluxo_ano_atual, fluxo_total

def criar_grafico(dados, titulo):
    """Cria um gráfico interativo de barras e linhas para visualização dos dados de fluxo usando Plotly"""
    # Formatando as datas para português
    dados_formatados = dados.copy()
    dados_formatados['Data_Formatada'] = dados_formatados['Data'].dt.strftime('%d/%m/%Y')
    
    # Criando um gráfico com dois eixos Y
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Adicionando dados do fluxo estrangeiro como barras
    fig.add_trace(
        go.Bar(
            x=dados_formatados['Data'],
            y=dados_formatados['Estrangeiro'],
            name="Estrangeiro",
            marker_color='#58FFE9',
            opacity=0.8,
            hovertemplate='Data: %{x|%d/%m/%Y}<br>Valor: R$ %{y:.2f} milhões<extra></extra>'
        ),
        secondary_y=False,
    )
    
    # Adicionando dados do Ibovespa como linha
    fig.add_trace(
        go.Scatter(
            x=dados_formatados['Data'],
            y=dados_formatados['Ibovespa'],
            name="Ibovespa",
            line=dict(color='white', width=2),
            hovertemplate='Data: %{x|%d/%m/%Y}<br>Ibovespa: %{y:.2f} pontos<extra></extra>'
        ),
        secondary_y=True,
    )
    
    # Atualizando layout e eixos
    fig.update_layout(
        title=titulo,
        annotations=[dict(
            x=0.5,
            y=-0.15,
            xref="paper",
            yref="paper",
            text="",
            showarrow=False
        )],
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        height=600
    )
    
    fig.update_xaxes(
        title_text="Período",
        tickangle=45,
        rangeslider_visible=False,
        tickformat="%d/%m/%Y",
        hoverformat="%d/%m/%Y"
    )
    
    fig.update_yaxes(
        title_text="Estrangeiro (Milhões R$)",
        secondary_y=False
    )
    
    fig.update_yaxes(
        title_text="Ibovespa (pontos)",
        secondary_y=True
    )
    
    return fig

def main():
    """Função principal da aplicação Streamlit"""
    # Cabeçalho com título e logo
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("Fluxo Estrangeiro de Investimentos na B3")
        st.write("AfterMarketFL.com.br")
    with col2:
        st.image("amfl_selo_gradiente_sem_fundo.png", width=150)
    
    # Remover barra lateral de opções
    # st.sidebar.title("Opções")
    # atualizar_dados = st.sidebar.button("Atualizar Dados")
    
    # Carregar dados - sem botão de atualizar por enquanto
    atualizar_dados = False
    fluxo_completo, fluxo_ano_atual, fluxo_total = carregar_dados(atualizar=atualizar_dados)
    
    # Obter a data mais recente dos dados
    ultima_data = fluxo_completo["Data"].max().strftime("%d/%m/%Y")
    st.write(f"Dados atualizados até: {ultima_data}")
    
    # Mostrar métricas relevantes
    col1, col2, col3 = st.columns(3)
    
    with col1:
        fluxo_ano = fluxo_ano_atual["Estrangeiro"].iloc[-1]
        st.metric("Fluxo Estrangeiro Acumulado no Ano", f"R$ {fluxo_ano:.2f} milhões")

    with col2:
        ultimo_fluxo_diario = fluxo_completo["Estrangeiro"].iloc[-1]
        st.metric("Último Valor Diário", f"R$ {ultimo_fluxo_diario:.2f} milhões")
        
    with col3:
        ibov_atual = fluxo_completo["Ibovespa"].iloc[-1]
        ibov_anterior = fluxo_completo["Ibovespa"].iloc[-2]
        delta = (ibov_atual - ibov_anterior) / ibov_anterior * 100
        st.metric("Ibovespa Atual", f"{ibov_atual:.2f} pontos", f"{delta:.2f}%")
    
    # Tabs para diferentes visualizações
    tab1, tab2, tab3 = st.tabs(["Fluxo Acumulado", "Fluxo Diário", "Dados"])
    
    with tab1:
        ano_atual = datetime.datetime.now().year
        
        # Título e botão de atualização lado a lado
        col1, col2 = st.columns([3, 1])
        with col1:
            st.header(f"Fluxo Estrangeiro em {ano_atual}")
        with col2:
            atualizar_dados = st.button("Atualizar Dados")
            if atualizar_dados:
                with st.spinner("Atualizando dados do mercado..."):
                    fluxo_completo, fluxo_ano_atual, fluxo_total = carregar_dados(atualizar=True)
        
        fig_ano_atual = criar_grafico(
            fluxo_ano_atual, 
            f"Fluxo Estrangeiro de Investimentos Acumulados na B3"
        )
        st.plotly_chart(fig_ano_atual, use_container_width=True)
    
    with tab2:
        st.header("Fluxo Estrangeiro Diário")
        
        # Configurando a visualização para dados diários (não acumulados)
        dados_diarios = fluxo_completo.copy()
        
        # Criando gráfico específico para dados diários
        fig_diario = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Formatando as datas para português
        dados_diarios['Data_Formatada'] = dados_diarios['Data'].dt.strftime('%d/%m/%Y')
        
        # Adicionando dados do fluxo estrangeiro diário como barras
        fig_diario.add_trace(
            go.Bar(
                x=dados_diarios['Data'],
                y=dados_diarios['Estrangeiro'],
                name="Estrangeiro",
                marker_color='#58FFE9',
                opacity=0.8,
                hovertemplate='Data: %{x|%d/%m/%Y}<br>Valor: R$ %{y:.2f} milhões<extra></extra>'
            ),
            secondary_y=False,
        )
        
        # Adicionando dados do Ibovespa como linha
        fig_diario.add_trace(
            go.Scatter(
                x=dados_diarios['Data'],
                y=dados_diarios['Ibovespa'],
                name="Ibovespa",
                line=dict(color='white', width=2),
                hovertemplate='Data: %{x|%d/%m/%Y}<br>Ibovespa: %{y:.2f} pontos<extra></extra>'
            ),
            secondary_y=True,
        )
        
        # Atualizando layout e eixos para dados diários
        fig_diario.update_layout(
            title="Fluxo Estrangeiro de Investimentos Diários na B3",
            annotations=[dict(
                x=0.5,
                y=-0.15,
                xref="paper",
                yref="paper",
                text="",
                showarrow=False
            )],
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5
            ),
            height=600
        )
        
        fig_diario.update_xaxes(
            title_text="Período",
            tickangle=45,
            rangeslider_visible=False,
            tickformat="%d/%m/%Y",
            hoverformat="%d/%m/%Y"
        )
        
        fig_diario.update_yaxes(
            title_text="Estrangeiro Diário (Milhões R$)",
            secondary_y=False
        )
        
        fig_diario.update_yaxes(
            title_text="Ibovespa (pontos)",
            secondary_y=True
        )
        
        st.plotly_chart(fig_diario, use_container_width=True)
        
        # Adicionando métricas relevantes para os dados diários
        st.subheader("Métricas Diárias")
        col1, col2 = st.columns(2)
        
        with col1:
            ultimo_valor = dados_diarios["Estrangeiro"].iloc[-1]
            st.metric("Último Valor Diário", f"R$ {ultimo_valor:.2f} milhões")
        
        with col2:
            max_valor = dados_diarios["Estrangeiro"].max()
            max_data = dados_diarios.loc[dados_diarios["Estrangeiro"] == max_valor, "Data"].iloc[0].strftime("%d/%m/%Y")
            st.metric("Maior Fluxo Diário", f"R$ {max_valor:.2f} milhões", f"em {max_data}")
    
    with tab3:
        st.header("Dados Brutos")
        
        # Seleção de dataset para visualização
        dataset = st.selectbox(
            "Selecione o conjunto de dados:",
            ["Fluxo Diário", "Fluxo Ano Convertido em Dólar", "Fluxo Total Acumulado"]
        )
        
        if dataset == "Fluxo Diário":
            st.dataframe(fluxo_completo)
        elif dataset == "Fluxo Ano Convertido em Dólar":
            st.dataframe(fluxo_ano_atual)
        else:
            st.dataframe(fluxo_total)
        
        # Opção para download dos dados
        st.download_button(
            label="Baixar dados como CSV",
            data=fluxo_completo.to_csv(index=False),
            file_name="fluxo_estrangeiro_b3.csv",
            mime="text/csv",
        )

# ==============================
### Output para verificar os resultados

if __name__ == "__main__":
    main()







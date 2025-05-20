"""
Fluxo Estrangeiro de Investimentos na B3 - Coleta de Dados
Este script coleta dados de fluxo estrangeiro e cotações do mercado financeiro.
"""

# Bibliotecas
import pandas as pd
import yfinance as yf
import requests
import os
import datetime
import lxml

def criar_pasta_dados():
    """Cria a pasta 'Dados' se não existir"""
    pasta = "Dados"
    if not os.path.exists(pasta):
        os.makedirs(pasta)
    return pasta

def coletar_dados_fluxo():
    """Coleta dados de fluxo estrangeiro da B3"""
    url = "https://www.dadosdemercado.com.br/fluxo"
    response = requests.get(url)
    dados_da_bolsa = pd.read_html(response.text)[0]
    
    # Limpeza inicial dos dados
    dados_da_bolsa["Estrangeiro"] = dados_da_bolsa["Estrangeiro"].str.replace("mi", "").str.strip()
    dados_da_bolsa["Inst. Financeira"] = dados_da_bolsa["Inst. Financeira"].str.replace("mi", "").str.strip()
    dados_da_bolsa["Pessoa física"] = dados_da_bolsa["Pessoa física"].str.replace("mi", "").str.strip()
    dados_da_bolsa["Institucional"] = dados_da_bolsa["Institucional"].str.replace("mi", "").str.strip()
    dados_da_bolsa["Outros"] = dados_da_bolsa["Outros"].str.replace("mi", "").str.strip()
    
    # Convertendo para o formato correto
    dados_da_bolsa["Data"] = pd.to_datetime(dados_da_bolsa["Data"], format="%d/%m/%Y")
    dados_da_bolsa = dados_da_bolsa.sort_values(by="Data")
    
    # Convertendo valores para float
    float_columns = ["Estrangeiro", "Institucional", "Pessoa física", "Inst. Financeira", "Outros"]
    for column in float_columns:
        dados_da_bolsa[column] = dados_da_bolsa[column] \
            .str.replace(".", "") \
            .str.replace(",", ".") \
            .astype(float)
    
    return dados_da_bolsa

def coletar_cotacoes(dados_da_bolsa):
    """Coleta cotações do Ibovespa e Dólar"""
    # Define a data de início como um dia antes do primeiro registro de dados
    data_busca = dados_da_bolsa["Data"].iloc[1] - pd.Timedelta(days=1)
    
    # Download dos dados das cotações
    cotacoes = yf.download(["^BVSP", "BRL=X"], start=data_busca, auto_adjust=False)["Adj Close"]
    
    # Preparação do dataframe de cotações
    cotacoes_pd = pd.DataFrame(cotacoes.reset_index()).dropna()
    cotacoes_pd.columns = ["Data", "Dólar", "Ibovespa"]
    
    # Convertendo fuso horário
    cotacoes_pd["Data"] = cotacoes_pd["Data"].dt.tz_localize(None)
    
    return cotacoes_pd


# ==============================
### Output para verificar os resultados


if __name__ == "__main__":
    today = datetime.date.today()
    print(f"Iniciando coleta de dados: {today}")
    
    # Cria pasta para armazenar os dados
    pasta = criar_pasta_dados()
    
    # Coleta dados do fluxo estrangeiro
    dados_da_bolsa = coletar_dados_fluxo()
    
    # Salva os dados em formato parquet
    dados_da_bolsa.to_parquet(f"{pasta}/dados_da_bolsa.parquet")
    print(f"Dados do fluxo estrangeiro salvos em {pasta}/dados_da_bolsa.parquet")
    
    # Coleta cotações
    cotacoes = coletar_cotacoes(dados_da_bolsa)
    cotacoes.to_parquet(f"{pasta}/cotacoes.parquet")
    print(f"Cotações salvas em {pasta}/cotacoes.parquet")
    
    print("Coleta de dados concluída com sucesso!")

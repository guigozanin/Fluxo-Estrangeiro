"""
Fluxo Estrangeiro de Investimentos na B3 - Coleta de Dados
Este script coleta dados de fluxo estrangeiro e cotações do mercado financeiro.
"""

import sys
import os
import datetime
from io import StringIO

import pandas as pd
import yfinance as yf
import requests


def criar_pasta_dados():
    pasta = "Dados"
    if not os.path.exists(pasta):
        os.makedirs(pasta)
    return pasta


def coletar_dados_fluxo():
    url = "https://www.dadosdemercado.com.br/fluxo"
    response = requests.get(url, timeout=15)
    response.raise_for_status()

    tabelas = pd.read_html(StringIO(response.text))
    if not tabelas:
        raise ValueError("Nenhuma tabela encontrada na pagina de fluxo")
    dados_da_bolsa = tabelas[0]

    expected_cols = ["Estrangeiro", "Inst. Financeira", "Pessoa fisica", "Institucional", "Outros"]
    alt_cols = {"Pessoa fisica": "Pessoa física"}

    for col in expected_cols:
        real_col = alt_cols.get(col, col)
        match = next((c for c in dados_da_bolsa.columns if c.strip() == real_col or c.strip() == col), None)
        if match:
            dados_da_bolsa[match] = dados_da_bolsa[match].astype(str).str.replace("mi", "", regex=False).str.strip()
        else:
            dados_da_bolsa[real_col] = pd.NA

    if "Data" not in dados_da_bolsa.columns:
        raise KeyError(f"Coluna 'Data' nao encontrada. Colunas: {list(dados_da_bolsa.columns)}")

    dados_da_bolsa["Data"] = pd.to_datetime(dados_da_bolsa["Data"], dayfirst=True, errors='coerce')
    dados_da_bolsa = dados_da_bolsa.sort_values(by="Data")

    float_cols = [c for c in dados_da_bolsa.columns if c in
                  ["Estrangeiro", "Inst. Financeira", "Pessoa física", "Institucional", "Outros"]]
    for column in float_cols:
        s = dados_da_bolsa[column].astype(str) \
            .str.replace(".", "", regex=False) \
            .str.replace(",", ".", regex=False)
        dados_da_bolsa[column] = pd.to_numeric(s, errors='coerce')

    return dados_da_bolsa


def coletar_cotacoes(dados_da_bolsa):
    if "Data" not in dados_da_bolsa.columns or dados_da_bolsa["Data"].dropna().empty:
        raise ValueError("dados_da_bolsa nao contem datas validas")

    primeiro_registro = dados_da_bolsa["Data"].dropna().iloc[0]
    data_busca = primeiro_registro - pd.Timedelta(days=1)

    cotacoes_raw = yf.download(
        ["^BVSP", "BRL=X"],
        start=data_busca,
        auto_adjust=False,
        progress=False
    )

    print(f"yfinance colunas: {list(cotacoes_raw.columns)}")

    # Extrai 'Adj Close' se disponivel, senao usa 'Close'
    if "Adj Close" in cotacoes_raw.columns:
        adj = cotacoes_raw["Adj Close"]
    elif "Close" in cotacoes_raw.columns:
        adj = cotacoes_raw["Close"]
    else:
        adj = cotacoes_raw

    cotacoes_pd = adj.reset_index()

    # Achata MultiIndex se necessario
    if isinstance(cotacoes_pd.columns, pd.MultiIndex):
        cotacoes_pd.columns = [" ".join(str(s) for s in col).strip() for col in cotacoes_pd.columns]

    print(f"Colunas apos reset_index: {list(cotacoes_pd.columns)}")

    cols = cotacoes_pd.columns.tolist()
    rename_map = {cols[0]: "Data"}
    for c in cols[1:]:
        name = str(c)
        if "BRL=X" in name or ("BRL" in name and "BVSP" not in name):
            rename_map[c] = "Dolar"
        if "^BVSP" in name or "BVSP" in name:
            rename_map[c] = "Ibovespa"

    cotacoes_pd = cotacoes_pd.rename(columns=rename_map)

    # Renomeia Dolar para Dólar
    if "Dolar" in cotacoes_pd.columns:
        cotacoes_pd = cotacoes_pd.rename(columns={"Dolar": "Dólar"})

    cotacoes_pd = cotacoes_pd.dropna(how='all')

    cotacoes_pd["Data"] = pd.to_datetime(cotacoes_pd["Data"], errors='coerce')
    if cotacoes_pd["Data"].dt.tz is not None:
        cotacoes_pd["Data"] = cotacoes_pd["Data"].dt.tz_convert(None)

    print(f"Cotacoes coletadas: {cotacoes_pd.shape}, colunas: {list(cotacoes_pd.columns)}")
    return cotacoes_pd


if __name__ == "__main__":
    today = datetime.date.today()
    print(f"Iniciando coleta de dados: {today}")
    print(f"pandas: {pd.__version__}")
    print(f"yfinance: {yf.__version__}")

    try:
        pasta = criar_pasta_dados()

        dados_da_bolsa = coletar_dados_fluxo()
        dados_da_bolsa.to_parquet(f"{pasta}/dados_da_bolsa.parquet")
        print(f"Dados do fluxo estrangeiro salvos em {pasta}/dados_da_bolsa.parquet")

        cotacoes = coletar_cotacoes(dados_da_bolsa)
        cotacoes.to_parquet(f"{pasta}/dados_da_bolsa_final.parquet")
        print(f"Cotacoes salvas em {pasta}/dados_da_bolsa_final.parquet")

        print("Coleta de dados concluida com sucesso!")
    except Exception as e:
        print(f"Erro durante coleta: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

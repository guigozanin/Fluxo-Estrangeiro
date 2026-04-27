"""
Fluxo Estrangeiro de Investimentos na B3 - Processamento de Dados
Este script processa os dados coletados e gera as análises necessárias.
"""

# Bibliotecas
import pandas as pd
import os
import datetime

def carregar_dados(pasta="Dados"):
    """Carrega os dados coletados"""
    try:
        dados_da_bolsa = pd.read_parquet(f"{pasta}/dados_da_bolsa.parquet")
        cotacoes = pd.read_parquet(f"{pasta}/dados_da_bolsa_final.parquet")
        return dados_da_bolsa, cotacoes
    except FileNotFoundError:
        print("Arquivos de dados não encontrados. Execute primeiro o script de coleta de dados.")
        return None, None

def _remover_timezone(serie):
    """Remove timezone de uma Series de datas, se presente."""
    try:
        if serie.dt.tz is not None:
            return serie.dt.tz_localize(None)
    except Exception:
        pass
    return serie

def mesclar_dados(dados_da_bolsa, cotacoes):
    """Mescla os dados de fluxo com as cotações"""
    # Verifica se a coluna 'Data' existe no dataframe de cotações
    if "Data" not in cotacoes.columns:
        raise KeyError(
            f"Coluna 'Data' não encontrada em cotacoes. Colunas disponíveis: {list(cotacoes.columns)}"
        )

    # Garantir que os formatos de data são compatíveis
    dados_da_bolsa["Data"] = _remover_timezone(pd.to_datetime(dados_da_bolsa["Data"], errors='coerce'))
    cotacoes["Data"] = _remover_timezone(pd.to_datetime(cotacoes["Data"], errors='coerce'))
    
    # Mesclar dados
    fluxo_mais_ibov = pd.merge(cotacoes, dados_da_bolsa, on="Data", how="left")

    # Preencher Dólar com forward/backward fill caso haja falhas de download (ex: rate limit)
    if "Dólar" in fluxo_mais_ibov.columns:
        fluxo_mais_ibov["Dólar"] = (
            fluxo_mais_ibov["Dólar"].ffill().bfill()
        )

    # Remover apenas linhas sem Ibovespa ou Estrangeiro (colunas essenciais)
    fluxo_mais_ibov.dropna(subset=["Ibovespa", "Estrangeiro"], inplace=True)

    # Calcular fluxo em dólar
    fluxo_mais_ibov["Estrangeiro_em_dolar"] = fluxo_mais_ibov["Estrangeiro"] / fluxo_mais_ibov["Dólar"]
    
    return fluxo_mais_ibov

def calcular_fluxo_acumulado(dados_fluxo, ano_filtro=None):
    """Calcula o fluxo acumulado para o período desejado"""
    # Filtrar por ano se especificado
    if ano_filtro:
        dados_filtrados = dados_fluxo[dados_fluxo["Data"].dt.year == ano_filtro]
    else:
        dados_filtrados = dados_fluxo
    
    # Extrair os componentes necessários
    indice = dados_filtrados["Data"]
    ibov = dados_filtrados["Ibovespa"]
    estrangeiro_acumulado = dados_filtrados["Estrangeiro"].cumsum()
    estrangeiro_dolar_acumulado = dados_filtrados["Estrangeiro_em_dolar"].cumsum()
    
    # Criar dataframe com dados acumulados
    fluxo_acumulado = pd.concat([
        indice, 
        ibov,
        estrangeiro_acumulado,
        estrangeiro_dolar_acumulado
    ], axis=1)
    
    fluxo_acumulado.columns = ["Data", "Ibovespa", "Estrangeiro", "Estrangeiro_em_dolar"]
    fluxo_acumulado = fluxo_acumulado.dropna()
    
    return fluxo_acumulado

def processar_dados_para_analise():
    """Processa todos os dados para análise"""
    pasta = "Dados"
    
    # Carregar dados
    dados_da_bolsa, cotacoes = carregar_dados(pasta)
    if dados_da_bolsa is None:
        return
    
    # Mesclar dados
    fluxo_completo = mesclar_dados(dados_da_bolsa, cotacoes)
    
    # Salvar dados mesclados
    fluxo_completo.to_parquet(f"{pasta}/fluxo_completo.parquet")
    
    # Obter ano atual
    ano_atual = datetime.datetime.now().year
    
    # Calcular dados acumulados para o ano atual
    fluxo_ano_atual = calcular_fluxo_acumulado(fluxo_completo, ano_atual)
    fluxo_ano_atual.to_parquet(f"{pasta}/fluxo_ano_atual.parquet")
    
    # Calcular dados acumulados totais
    fluxo_total = calcular_fluxo_acumulado(fluxo_completo)
    fluxo_total.to_parquet(f"{pasta}/fluxo_total.parquet")
    
    return fluxo_ano_atual

# ==============================
### Output para verificar os resultados

if __name__ == "__main__":
    today = datetime.date.today()
    print(f"Iniciando processamento de dados: {today}")
    
    fluxo_atual = processar_dados_para_analise()
    if fluxo_atual is not None:
        print("Processamento de dados concluído com sucesso!")
        print(f"Últimos registros do fluxo do ano atual:\n{fluxo_atual.tail()}")

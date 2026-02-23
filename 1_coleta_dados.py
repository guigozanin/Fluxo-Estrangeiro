"""
Fluxo Estrangeiro de Investimentos na B3 - Coleta de Dados
Este script coleta dados de fluxo estrangeiro e cotações do mercado financeiro.
"""

# Verifica se sendo executado diretamente ou importado como módulo
import sys
import os
import datetime
import pandas as pd

def criar_pasta_dados():
    """Cria a pasta 'Dados' se não existir"""
    pasta = "Dados"
    if not os.path.exists(pasta):
        os.makedirs(pasta)
    return pasta


def coletar_dados_fluxo():
    """Coleta dados de fluxo estrangeiro da B3.

    Observações importantes:
    - Importa pandas/requests/lxml apenas quando a função é chamada para evitar
      falhas no import em tempo de inicialização do app.
    - Fornece mensagens de erro claras se dependências estiverem faltando.
    - Faz parsing mais tolerante (errors='coerce') e verifica existência de colunas.
    """
    try:
        import pandas as pd
    except Exception as e:
        raise ImportError(
            "O módulo 'pandas' não está instalado. Adicione 'pandas' em requirements.txt e redeploy ou instale no ambiente: `pip install pandas`"
        ) from e

    try:
        import requests
    except Exception as e:
        raise ImportError(
            "O módulo 'requests' não está instalado. Adicione 'requests' em requirements.txt e redeploy ou instale no ambiente: `pip install requests`"
        ) from e

    # pd.read_html pode usar lxml/builtin; noiçe se lxml não estiver presente, read_html pode falhar.
    try:
        import lxml  # noqa: F401
    except Exception:
        # Não falhar aqui — read_html tentará outros parsers, mas deixa mensagem para o usuário
        pass

    url = "https://www.dadosdemercado.com.br/fluxo"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Falha ao requisitar {url}: {e}") from e

    try:
        tabelas = pd.read_html(response.text)
        if not tabelas:
            raise ValueError("Nenhuma tabela encontrada na página de fluxo")
        dados_da_bolsa = tabelas[0]
    except Exception as e:
        raise RuntimeError(f"Falha ao extrair tabela HTML de {url}: {e}") from e

    # Normalizar nomes de colunas esperados - adiciona colunas vazias se não existirem
    expected_cols = ["Estrangeiro", "Inst. Financeira", "Pessoa física", "Institucional", "Outros"]
    for col in expected_cols:
        if col in dados_da_bolsa.columns:
            # garante string antes de manipular e evita warnings sobre regex
            dados_da_bolsa[col] = dados_da_bolsa[col].astype(str).str.replace("mi", "", regex=False).str.strip()
        else:
            dados_da_bolsa[col] = pd.NA

    # Data
    if "Data" not in dados_da_bolsa.columns:
        raise KeyError("Coluna 'Data' não encontrada na tabela de fluxo")

    dados_da_bolsa["Data"] = pd.to_datetime(dados_da_bolsa["Data"], dayfirst=True, errors='coerce')
    dados_da_bolsa = dados_da_bolsa.sort_values(by="Data")

    # Conversão para float mais tolerante
    float_columns = expected_cols
    for column in float_columns:
        if column in dados_da_bolsa.columns:
            s = dados_da_bolsa[column].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
            dados_da_bolsa[column] = pd.to_numeric(s, errors='coerce')

    return dados_da_bolsa


def coletar_cotacoes(dados_da_bolsa):
    """Coleta cotações do Ibovespa e Dólar.

    Importa yfinance e pandas dentro da função. Lida com diferentes formatos de retorno do yfinance.
    """
    try:
        import pandas as pd
    except Exception as e:
        raise ImportError(
            "O módulo 'pandas' não está instalado. Adicione 'pandas' em requirements.txt e redeploy ou instale no ambiente: `pip install pandas`"
        ) from e

    try:
        import yfinance as yf
    except Exception as e:
        raise ImportError(
            "O módulo 'yfinance' não está instalado. Adicione 'yfinance' em requirements.txt e redeploy ou instale no ambiente: `pip install yfinance`"
        ) from e

    # Assegura que temos pelo menos uma data válida
    if "Data" not in dados_da_bolsa.columns or dados_da_bolsa["Data"].dropna().empty:
        raise ValueError("dados_da_bolsa não contém datas válidas para definir o período de busca")

    primeiro_registro = dados_da_bolsa["Data"].dropna().iloc[0]
    data_busca = primeiro_registro - pd.Timedelta(days=1)

    try:
        cotacoes_raw = yf.download(["^BVSP", "BRL=X"], start=data_busca, auto_adjust=False, progress=False)
    except Exception as e:
        raise RuntimeError(f"Falha ao baixar cotações via yfinance: {e}") from e

    # Tenta extrair 'Adj Close' quando presente (yfinance costuma retornar MultiIndex)
    try:
        if isinstance(cotacoes_raw, pd.DataFrame) and "Adj Close" in cotacoes_raw.columns:
            adj = cotacoes_raw["Adj Close"]
        else:
            # Pode já estar no formato flat
            adj = cotacoes_raw
    except Exception:
        adj = cotacoes_raw

    # Preparação do dataframe final
    cotacoes_pd = pd.DataFrame(adj.reset_index())
    cotacoes_pd = cotacoes_pd.dropna(how='all')

    # Normaliza nomes (tenta detectar colunas BRL=X e IBOV/BVSP)
    cols = cotacoes_pd.columns.tolist()
    # Garante nomes mínimos
    if len(cols) >= 3:
        # Mantém 'Data' como primeira coluna
        cotacoes_pd.columns = [cols[0]] + cols[1:]
    # Renomeia colunas conhecidas
    rename_map = {}
    for c in cotacoes_pd.columns[1:]:
        name = str(c)
        if "BRL=X" in name or "BRL=" in name or "BRL" in name:
            rename_map[c] = "Dólar"
        if "^BVSP" in name or "BVSP" in name or "IBOV" in name or "IBOVESPA" in name:
            rename_map[c] = "Ibovespa"
    cotacoes_pd = cotacoes_pd.rename(columns=rename_map)

    # Converte Data para timezone-naive
    try:
        cotacoes_pd["Data"] = pd.to_datetime(cotacoes_pd["Data"], errors='coerce')
        cotacoes_pd["Data"] = cotacoes_pd["Data"].dt.tz_localize(None)
    except Exception:
        # se falhar, mantém como está mas avisa
        pass

    return cotacoes_pd


# ==============================
### Output para verificar os resultados


if __name__ == "__main__":
    today = datetime.date.today()
    print(f"Iniciando coleta de dados: {today}")

    try:
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
    except ImportError as ie:
        # Mensagem amigável e saída limpa — evita crash silencioso no ambiente de deploy
        print(f"Erro de dependência: {ie}")
        print("Verifique requirements.txt e redeploy. Saindo.")
        sys.exit(1)
    except Exception as e:
        print(f"Erro durante coleta: {e}")
        sys.exit(1)





########################################################################################################
# Fluxo Estrangeiro de Investimentos na B3
########################################################################################################


#Bibliotecas
from matplotlib import pyplot as plt
import pandas as pd
import yfinance as yf
import requests
import os
import datetime

today = datetime.date.today()
print(f"Bibliotecas carregadas com sucesso: {today}")


# Cria pasta dados se não existir
pasta = "Dados"
if not os.path.exists(pasta):
  os.makedirs(pasta)


###############
############### Fluxo Estrangeiro ###############
###############

url = "https://www.dadosdemercado.com.br/fluxo"
response = requests.get(url)
dados_da_bolsa = pd.read_html(response.text)[0]
dados_da_bolsa = pd.DataFrame(dados_da_bolsa)
dados_da_bolsa

dados_da_bolsa["Estrangeiro"] = dados_da_bolsa["Estrangeiro"].str.replace("mi", "").str.strip()
dados_da_bolsa["Inst. Financeira"] = dados_da_bolsa["Inst. Financeira"].str.replace("mi", "").str.strip()
dados_da_bolsa["Pessoa física"] = dados_da_bolsa["Pessoa física"].str.replace("mi", "").str.strip()
dados_da_bolsa["Institucional"] = dados_da_bolsa["Institucional"].str.replace("mi", "").str.strip()
dados_da_bolsa["Outros"] = dados_da_bolsa["Outros"].str.replace("mi", "").str.strip()
dados_da_bolsa["Data"] = pd.to_datetime(dados_da_bolsa["Data"], format = "%d/%m/%Y")
dados_da_bolsa = dados_da_bolsa.sort_values(by="Data")
dados_da_bolsa

float_columns = ["Estrangeiro", "Institucional", "Pessoa física", "Inst. Financeira", "Outros"]

for column in float_columns:
        dados_da_bolsa[column] = dados_da_bolsa[column] \
        .str.replace(".", "") \
        .str.replace(",", ".") \
        .astype(float)

dados_da_bolsa

dados_da_bolsa.to_parquet(f"{pasta}/dados_da_bolsa.parquet")


plt.figure(figsize=(10,8))
plt.bar(dados_da_bolsa["Data"], dados_da_bolsa["Estrangeiro"], color = "darkblue")
plt.xlabel("Período")
plt.ylabel("Em Milhões R$")
plt.title("Fluxo Estrangeiro de Investimentos na B3")
plt.xticks(rotation=45)
plt.tight_layout()
plt.figtext(0.01, 0.01,"Elaboração Gui Zanin, CFA - Base de dados: https: //www.dadosdemercado.com.br/")
plt.show()

indice_dados_da_bolsa = dados_da_bolsa["Data"]
fluxo_estrangeiro = dados_da_bolsa["Estrangeiro"].cumsum()
fluxo_somado = pd.concat([indice_dados_da_bolsa, fluxo_estrangeiro], axis = 1)
fluxo_somado

fluxo_somado.to_parquet(f"{pasta}/dados_da_bolsa_acumulado.parquet")

plt.figure(figsize=(10,8))
plt.bar(fluxo_somado["Data"], fluxo_somado["Estrangeiro"], color = "darkblue")
plt.xlabel("Período")
plt.ylabel("Em Milhões R$")
plt.title("Fluxo Estrangeiro de Investimentos na B3")
plt.xticks(rotation=45)
plt.tight_layout()
plt.figtext(0.01, 0.01,"Elaboração Gui Zanin, CFA - Base de dados: https: //www.dadosdemercado.com.br/")
plt.show()

dados_da_bolsa_curto = dados_da_bolsa[dados_da_bolsa["Data"] >= "2025/01/01"]
dados_da_bolsa_curto

plt.figure(figsize=(10,8))
plt.bar(dados_da_bolsa_curto["Data"], dados_da_bolsa_curto["Estrangeiro"], color = "darkblue")
plt.xlabel("Período")
plt.ylabel("Em Milhões R$")
plt.title("Fluxo Estrangeiro de Investimentos na B3 em 2024")
plt.xticks(rotation=45)
plt.tight_layout()
plt.figtext(0.01, 0.01,"Elaboração Gui Zanin, CFA - Base de dados: https: //www.dadosdemercado.com.br/")
plt.show()

indice_dados_da_bolsa2 = dados_da_bolsa_curto["Data"]
fluxo_estrangeiro2 = dados_da_bolsa_curto["Estrangeiro"].cumsum()
fluxo_somado2 = pd.concat([indice_dados_da_bolsa2, fluxo_estrangeiro2], axis = 1)
fluxo_somado2

plt.figure(figsize=(10,8))
plt.bar(fluxo_somado2["Data"], fluxo_somado2["Estrangeiro"], color = "darkblue")
plt.xlabel("Período")
plt.ylabel("Em Milhões R$")
plt.title("Fluxo Estrangeiro de Investimentos na B3")
plt.xticks(rotation=45)
plt.tight_layout()
plt.figtext(0.01, 0.01,"Elaboração Gui Zanin, CFA - Base de dados: https: //www.dadosdemercado.com.br/")
plt.show()

data_busca = dados_da_bolsa["Data"].iloc[1]
data_busca = data_busca - pd.Timedelta(days=1)
data_busca

cotacoes = yf.download(["^BVSP", "BRL=X"], start=data_busca, auto_adjust=False)["Adj Close"]

cotacoes_pd = pd.DataFrame(cotacoes.reset_index()).dropna()
cotacoes_pd.columns = ["Data", "Dólar", "Ibovespa"]
cotacoes_pd

# Merging Data

#Converting Data to the same
cotacoes_pd["Data"] = cotacoes_pd["Data"].dt.tz_localize(None)
dados_da_bolsa["Data"] = dados_da_bolsa["Data"].dt.tz_localize(None)

# Merge
fluxo_mais_ibov = pd.merge(cotacoes_pd, dados_da_bolsa, on="Data", how="left")
fluxo_mais_ibov.dropna()
fluxo_mais_ibov

# Fluxo em Dólar
fluxo_mais_ibov["Estrangeiro_em_dolar"] = fluxo_mais_ibov["Estrangeiro"] / fluxo_mais_ibov["Dólar"]
fluxo_mais_ibov

fluxo_mais_ibov_pd = fluxo_mais_ibov
fluxo_mais_ibov_pd["Data"] = pd.to_datetime(fluxo_mais_ibov_pd["Data"])


# Criando o gráfico
fig, ax1 = plt. subplots (figsize = (10,6))
# Plotando os dados do "Estrangeiro" como barras
ax1.bar (fluxo_mais_ibov_pd['Data'], fluxo_mais_ibov_pd['Estrangeiro'], color='darkblue', label='Estrangeiro')
# Criando um segundo eixo Y para os dados do "Ibovespa"
ax2 = ax1.twinx ()
ax2.plot(fluxo_mais_ibov_pd['Data'], fluxo_mais_ibov_pd['Ibovespa'], color='gold', label='Ibovespa')
# Adicionando títulos e rótulos
ax1.set_xlabel('Período')
ax1.set_ylabel('Estrangeiro', color= 'darkblue')
ax2.set_ylabel('Ibovespa', color='gold')
# Adicionando legendas
ax1. legend (loc= 'upper left')
ax2. legend(loc= 'upper right')
plt.figtext (0.01, 0.01, "Fonte: Gui Zanin, CFA - Base de dados: https: //www.dadosdemercado.com.br/")
# Ajustando a apresentação do gráfico fig.tight layout ()
plt.show()

fluxo_mais_ibov_pd

indice_fluxo_mais_ibov_pd = fluxo_mais_ibov_pd["Data"]
ibov_fluxo_mais_ibov_pd = fluxo_mais_ibov_pd["Ibovespa"]
estrangeiro_fluxo_mais_ibov_pd = fluxo_mais_ibov_pd["Estrangeiro"].cumsum()
estrangeiro_dolar_fluxo_mais_ibov_pd = fluxo_mais_ibov_pd["Estrangeiro_em_dolar"].cumsum()
fluxo_somado3 = pd.concat([indice_fluxo_mais_ibov_pd, ibov_fluxo_mais_ibov_pd,estrangeiro_fluxo_mais_ibov_pd,estrangeiro_dolar_fluxo_mais_ibov_pd ], axis = 1)
fluxo_somado3.dropna()
fluxo_somado3

# Criando o gráfico
fig, ax1 = plt. subplots (figsize = (8,8))
# Plotando os dados do "Estrangeiro" como barras
ax1.bar (fluxo_somado3['Data'], fluxo_somado3['Estrangeiro'], color='silver', label='Estrangeiro')
# Criando um segundo eixo Y para os dados do "Ibovespa"
ax2 = ax1.twinx ()
ax2.plot(fluxo_somado3['Data'], fluxo_somado3['Ibovespa'], color='darkred', label='Ibovespa')                           
# Adicionando títulos e rótulos
ax1.set_xlabel('Período')
ax1.set_ylabel('Estrangeiro', color= 'silver')
ax2.set_ylabel('Ibovespa', color='darkred')
# Adicionando legendas
ax1. legend (loc= 'upper left')
ax2. legend(loc= 'upper right')
#plt.figtext (0.01, 0.01, "Fonte: Gui Zanin, CFA - Base de dados: https: //www.dadosdemercado.com.br/")
plt.title("Fluxo Estrangeiro de Investimentos na B3")
# Ajustando a apresentação do gráfico fig.tight layout ()
# Salvando o gráfico em um arquivo
# plt.savefig('graficos/grafico_fluxo_ibov.png')
plt.tight_layout()
plt.show()

dados_da_bolsa_curto2 = fluxo_mais_ibov_pd[fluxo_mais_ibov_pd["Data"] >= "2025/01/01"]
dados_da_bolsa_curto2

indice_fluxo_mais_ibov_pd2 = dados_da_bolsa_curto2["Data"]
ibov_fluxo_mais_ibov_pd2 = dados_da_bolsa_curto2["Ibovespa"]
estrangeiro_fluxo_mais_ibov_pd2 = dados_da_bolsa_curto2["Estrangeiro"].cumsum()
estrangeiro_dolar_fluxo_mais_ibov_pd2 = dados_da_bolsa_curto2["Estrangeiro_em_dolar"].cumsum()
fluxo_somado4 = pd.concat([indice_fluxo_mais_ibov_pd2, ibov_fluxo_mais_ibov_pd2,estrangeiro_fluxo_mais_ibov_pd2,estrangeiro_dolar_fluxo_mais_ibov_pd2], axis = 1)
fluxo_somado4.dropna()
#fluxo_somado4['Data'] = fluxo_somado4['Data'].dt.strftime('%d/%m/%Y')

fluxo_somado4


# The above Python code is creating a dual-axis plot using Matplotlib to visualize the data related to
# foreign investment flows ("Estrangeiro") and the performance of the Ibovespa index over time. Here
# is a breakdown of the code:
# Criando o gráfico
fig, ax1 = plt.subplots()

# Plotando os dados do "Estrangeiro" como barras
ax1.bar(fluxo_somado4['Data'], fluxo_somado4['Estrangeiro'], color='#58FFE9', label='Estrangeiro')

# Criando um segundo eixo Y para os dados do "Ibovespa"
ax2 = ax1.twinx()
ax2.plot(fluxo_somado4['Data'], fluxo_somado4['Ibovespa'], color='#050A16', label='Ibovespa')

# Adicionando títulos e rótulos
ax1.set_xlabel('Período')
ax1.set_ylabel('Estrangeiro (Milhões)', color='black')
ax2.set_ylabel('Ibovespa (pontos)', color='black')

# Ajustando a rotação dos rótulos do eixo X
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)


# Adicionando legendas
ax1.legend(loc='upper left')
ax2.legend(loc='upper right')

plt.figtext(0.5, 0.25, "Fonte: @AfterMarketFL", fontsize=12)
plt.title("Fluxo Estrangeiro de Investimentos na B3 em 2025")

# Ajustando a apresentação do gráfico
fig.tight_layout()

# Salvando o gráfico em um arquivo
# plt.savefig('graficos/grafico_fluxo_ibov2.png')
plt.show()


fluxo_somado4.to_parquet(f"{pasta}/dados_da_bolsa_final.parquet")
fluxo_somado4.tail()
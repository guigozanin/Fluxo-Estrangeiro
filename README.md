# Fluxo Estrangeiro de Investimentos na B3

Este projeto coleta, processa e visualiza dados de fluxo estrangeiro de investimentos na B3 (Bolsa de Valores do Brasil) por meio de uma aplicação Streamlit.

## Estrutura do Projeto

O projeto está dividido em três scripts principais:

1. `1_coleta_dados.py`: Coleta dados de fluxo estrangeiro e cotações do mercado financeiro
2. `2_processa_dados.py`: Processa os dados coletados e gera as análises necessárias
3. `3_app_streamlit.py`: Interface Streamlit para visualização dos dados

## Requisitos

As dependências do projeto estão listadas no arquivo `requirements.txt`. Para instalá-las, execute:

```bash
pip install -r requirements.txt
```

## Executando a Aplicação

Para executar a aplicação Streamlit, use o seguinte comando:

```bash
streamlit run 3_app_streamlit.py
```

A aplicação irá automaticamente verificar se os dados estão disponíveis. Caso não estejam, irá executar os scripts de coleta e processamento de dados.

## Atualizando os Dados

Os dados podem ser atualizados manualmente através da interface Streamlit ou executando os scripts individuais:

```bash
# Para coletar novos dados
python 1_coleta_dados.py

# Para processar os dados coletados
python 2_processa_dados.py
```

## Dados

Os dados processados são armazenados na pasta `Dados` no formato Parquet:

- `dados_da_bolsa.parquet`: Dados brutos de fluxo estrangeiro
- `cotacoes.parquet`: Dados de cotações do Ibovespa e Dólar
- `fluxo_completo.parquet`: Dados de fluxo mesclados com cotações
- `fluxo_ano_atual.parquet`: Dados de fluxo acumulados para o ano atual
- `fluxo_total.parquet`: Dados de fluxo acumulados para todo o período

## Autor

Guilherme Renato Rossler Zanin

## Fonte dos Dados

- Dados de Fluxo Estrangeiro: [Dados de Mercado](https://www.dadosdemercado.com.br/fluxo)
- Cotações: [Yahoo Finance](https://finance.yahoo.com/)

# Case Técnico iFood - Data Architecture

## Objetivo

Desenvolver uma solução ponta a ponta utilizando Databricks para ingestão, transformação e análise dos dados de corridas Yellow Taxi NYC entre janeiro e maio de 2023.

---

## Arquitetura

Arquitetura Medallion (Lakehouse)

Landing → Bronze → Silver → Gold → Dashboards → Insights

---

## Tecnologias Utilizadas

- Databricks Free Edition
- PySpark
- Delta Lake
- Unity Catalog
- SQL
- GitHub

---

## Estrutura da Solução

### Landing

Recebimento dos arquivos Parquet originais.

### Bronze

Consolidação dos arquivos e padronização de schema.

### Silver

Aplicação de regras de qualidade e enriquecimento dos dados.

### Gold

Camada analítica preparada para consumo de negócio.

---

## Perguntas Respondidas

### 1. Qual a média de valor total recebido por mês?

Resultado:

| Mês | Ticket Médio |
|------|------:|
| 2023-01 | 27.46 |
| 2023-02 | 27.37 |
| 2023-03 | 28.29 |
| 2023-04 | 28.78 |
| 2023-05 | 29.45 |

---

### 2. Qual a média de passageiros por hora em Maio/2023?

Resultado:

Variação entre 1.26 e 1.46 passageiros por corrida.

---

## Dashboards

- Evolução do Ticket Médio por Mês
- Média de Passageiros por Hora

---

## Como Executar

1. Criar volume no Unity Catalog.
2. Fazer upload dos arquivos Parquet na Landing.
3. Executar o notebook de ingestão.
4. Criar as camadas Bronze, Silver e Gold.
5. Executar as análises e dashboards.

---

## Autor

Fernanda Moura


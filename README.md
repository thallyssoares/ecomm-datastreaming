# 🛒 Kafka E-Commerce Datastream

Plataforma de Analytics de E-Commerce em Tempo Real (Clickstream)

## 🎯 Objetivo

Capturar, processar e analisar comportamento de usuários (cliques, visualizações, compras) em e-commerce fictício, demonstrando proficiência em:
- **Streaming**: Redpanda (Kafka) + Bytewax (Python)
- **Data Warehouse**: BigQuery (Bronze/Silver/Gold)
- **Transformação**: dbt Core (SQL)
- **Orquestração**: Dagster (Software-Defined Assets)
- **Qualidade**: Soda Core (Data Contracts)
- **Infra**: Docker Compose + Makefile

## 🏗️ Arquitetura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Simulador  │────▶│  Redpanda   │────▶│  Bytewax    │────▶│  BigQuery   │
│  (Python)   │     │  (Kafka)    │     │  Consumer   │     │  Bronze     │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                    │
                                                    ┌───────────────┴───────────────┐
                                                    ▼                               ▼
                                            ┌───────────────┐               ┌───────────────┐
                                            │   dbt Core    │               │    Soda       │
                                            │  (Silver/Gold)│               │  (Contracts)  │
                                            └───────┬───────┘               └───────┬───────┘
                                                    │                               │
                                                    └───────────────┬───────────────┘
                                                                    ▼
                                                            ┌───────────────┐
                                                            │    Dagster    │
                                                            │ (Orchestration)│
                                                            └───────┬───────┘
                                                                    ▼
                                                            ┌───────────────┐
                                                            │  Looker Studio│
                                                            │  (Dashboard)  │
                                                            └───────────────┘
```

## 📊 Camadas de Dados

| Camada | Dataset | Descrição | Atualização |
|--------|---------|-----------|-------------|
| **Bronze** | `bronze.raw_clickstream` | Eventos brutos (append-only streaming) | Tempo real |
| **Silver** | `silver.stg_clickstream` | Limpos, tipados, deduplicados, enriquecidos | Hourly (dbt) |
| **Silver** | `silver.stg_products` | Dimensão produtos (seed CSV) | Daily (dbt seed) |
| **Intermediate** | `silver.int_enriched_clickstream` | Clickstream + JOIN produtos | Hourly (dbt) |
| **Gold** | `gold.fct_daily_revenue` | Faturamento diário | Hourly (dbt) |
| **Gold** | `gold.fct_conversion_funnel` | Funil conversão (4 etapas) | Hourly (dbt) |
| **Gold** | `gold.fct_cart_abandonment` | Abandono por categoria | Hourly (dbt) |

## 🚀 Quick Start

### Pré-requisitos
- Docker + Docker Compose
- Google Cloud Project
- BigQuery API habilitada
- `gcloud` autenticado: `gcloud auth application-default login`

### 1. Clone e configure
```bash
git clone <repo>
cd kafka-ecom-datastream
cp .env.example .env
# Edite .env com seu GCP_PROJECT_ID
```

### 2. Suba a stack de streaming
```bash
make up
# Ou: docker-compose up -d --build
```

Verifique saúde:
```bash
make logs          # Logs de todos os serviços
make redpanda-console  # Abre UI do Kafka (porta 8080)
make kafka-topics  # Lista tópicos
```

### 3. Crie datasets/tabelas no BigQuery (manual ou Terraform)
```bash
# Via console ou:
gcloud bigquery datasets create bronze --location=US
gcloud bigquery datasets create silver --location=US
gcloud bigquery datasets create gold --location=US

# Tabela bronze.raw_clickstream será criada automaticamente pelo consumer
# Se preferir criar manual, use o schema em docs/schema_bronze.json
```

### 4. dbt - Transformação
```bash
# Configure profiles.yml
cd dbt_project
cp profiles.yml.example profiles.yml
# Edite profiles.yml com seu project_id

# Pipeline completo
make dbt-build
# Ou passo a passo:
make dbt-seed   # Carrega dim_products
make dbt-run    # Roda models
make dbt-test   # Valida qualidade
```

### 5. Dagster - Orquestração
```bash
make dagster-up
# UI: http://localhost:3000
```

### 6. Soda - Qualidade
```bash
make soda-scan
```

## 🛠️ Comandos Principais

```bash
# Infraestrutura
make up              # Sobe tudo
make down            # Para tudo
make logs            # Logs agregados
make clean           # Remove volumes/containers

# Desenvolvimento
make lint            # Ruff + MyPy
make fmt             # Formata código
make test            # Lint + dbt test

# dbt
make dbt-debug       # Testa conexão
make dbt-seed        # Seeds
make dbt-run         # Models
make dbt-test        # Testes
make dbt-build       # Seed + Run + Test
make dbt-docs        # Documentação

# Dagster
make dagster-up      # Sobe UI (porta 3000)
make dagster-down

# Soda
make soda-scan       # Roda checks

# Kafka
make redpanda-console  # UI Web
make kafka-topics    # Lista tópicos
make kafka-consome   # Consome eventos
```

## 📁 Estrutura do Projeto

```
kafka-ecom-datastream/
├── .github/workflows/     # CI/CD (futuro)
├── docker/                # Redpanda + Console
├── simulator/             # Gerador de eventos Python
├── consumer/              # Bytewax Kafka → BigQuery
├── dbt_project/           # Transformação SQL
│   ├── models/
│   │   ├── staging/       # Silver (stg_)
│   │   ├── intermediate/  # Silver enriquecido (int_)
│   │   └── marts/         # Gold (fct_)
│   ├── seeds/             # dim_products.csv
│   ├── macros/            # Macros dbt
│   └── tests/             # Testes customizados
├── dagster/               # Orquestração
│   ├── src/assets/        # Software-Defined Assets
│   ├── src/jobs/          # Jobs de execução
│   ├── src/schedules/     # Agendamentos cron
│   └── src/sensors/       # Sensors event-driven
├── soda/                  # Data Quality
│   ├── checks/            # Contratos YAML por tabela
│   └── configuration.yml  # Config BigQuery
├── docker-compose.yml     # Stack completa local
├── Makefile               # Comandos úteis
├── .env.example           # Template variáveis
└── README.md
```

## 🔑 Variáveis de Ambiente (.env)

| Variável | Descrição | Obrigatório |
|----------|-----------|-------------|
| `GCP_PROJECT_ID` | ID do projeto GCP | ✅ |
| `GCP_REGION` | Região GCP (us-central1) | ✅ |
| `BQ_LOCATION` | Location BigQuery (US) | ✅ |
| `KAFKA_BOOTSTRAP_SERVERS` | Broker Kafka | ✅ (default localhost:9092) |

## 🧪 Testes de Qualidade

### dbt Tests (schema.yml)
- `not_null` em chaves primárias
- `unique` em event_id
- `accepted_values` para event_type, device_type
- `dbt_expectations` para ranges (price >= 0)

### Soda Checks (YAML)
- **Freshness**: dados < 2h (streaming ativo)
- **Volume**: > 100 eventos/hora
- **Schema**: colunas obrigatórias
- **Anomaly**: detecção EWMA (queda brusca)

## 📈 Métricas de Negócio (Gold)

| Dashboard | Pergunta Respondida |
|-----------|---------------------|
| `fct_daily_revenue` | Faturamento bruto minuto a minuto? |
| `fct_conversion_funnel` | Funil: page_view → view_item → add_to_cart → purchase? |
| `fct_cart_abandonment` | Taxa abandono por categoria? |

## 🔧 Troubleshooting

### Redpanda não sobe
```bash
docker-compose logs redpanda
# Verifica memória: --memory=1G precisa de 2GB+ disponíveis
```

### Bytewax não conecta no BigQuery
```bash
# Verifica credenciais
gcloud auth application-default login
# Ou service account:
export GOOGLE_APPLICATION_CREDENTIALS=/path/key.json
```

### dbt falha com "table not found"
```bash
# Cria datasets primeiro
gcloud bigquery datasets create bronze --location=US
gcloud bigquery datasets create silver --location=US
gcloud bigquery datasets create gold --location=US
```

### Dagster não vê assets dbt
```bash
# Gere manifest.json
cd dbt_project && dbt parse
# Reinicie Dagster
make dagster-down && make dagster-up
```
## 📄 Licença

MIT

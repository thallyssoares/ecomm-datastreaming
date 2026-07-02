# Makefile - Comandos úteis para o projeto
# Uso: make <comando>

.PHONY: help up down logs clean test lint dbt-run dbt-test dbt-seed dagster-up soda-scan

# Default target
help:
	@echo "🛒 Kafka E-Commerce Datastream - Comandos disponíveis:"
	@echo ""
	@echo "Infraestrutura:"
	@echo "  make up              - Sobe toda a stack (Redpanda + Simulator + Consumer)"
	@echo "  make down            - Para e remove containers"
	@echo "  make logs            - Mostra logs de todos os serviços"
	@echo "  make logs-simulator  - Logs apenas do simulador"
	@echo "  make logs-consumer   - Logs apenas do consumer"
	@echo "  make clean           - Remove containers, volumes e networks"
	@echo ""
	@echo "Desenvolvimento:"
	@echo "  make test            - Roda testes (Python lint + dbt test)"
	@echo "  make lint            - Roda linters (ruff + mypy)"
	@echo "  make fmt             - Formata código (ruff format)"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt-debug       - Testa conexão dbt"
	@echo "  make dbt-seed        - Carrega seeds (dim_products)"
	@echo "  make dbt-run         - Roda models (staging + marts)"
	@echo "  make dbt-test        - Roda testes dbt"
	@echo "  make dbt-build       - Seed + run + test (pipeline completo)"
	@echo "  make dbt-docs        - Gera e serve documentação dbt"
	@echo ""
	@echo "Dagster:"
	@echo "  make dagster-up      - Sobe Dagster UI (porta 3000)"
	@echo "  make dagster-down    - Para Dagster"
	@echo ""
	@echo "Soda:"
	@echo "  make soda-scan       - Roda checks de qualidade"
	@echo ""
	@echo "Utilitários:"
	@echo "  make redpanda-console - Abre Redpanda Console no browser"
	@echo "  make kafka-topics    - Lista tópicos Kafka"
	@echo "  make kafka-consume   - Consome tópico ecommerce-events"

# ==========================================
# INFRAESTRUTURA
# ==========================================

up:
	docker-compose up -d --build

down:
	docker-compose down

logs:
	docker-compose logs -f

logs-simulator:
	docker-compose logs -f simulator

logs-consumer:
	docker-compose logs -f bytewax-consumer

logs-redpanda:
	docker-compose logs -f redpanda

clean:
	docker-compose down -v --remove-orphans
	docker network prune -f
	docker volume prune -f

# ==========================================
# DESENVOLVIMENTO
# ==========================================

lint:
	@echo "🔍 Rodando linters..."
	ruff check .
	ruff format --check .
	@echo "✅ Lint OK"

fmt:
	@echo "🎨 Formatando código..."
	ruff format .
	ruff check --fix .
	@echo "✅ Formatação OK"

test: lint
	@echo "🧪 Rodando testes..."
	cd dbt_project && dbt test
	@echo "✅ Testes OK"

# ==========================================
# DBT
# ==========================================

dbt-debug:
	cd dbt_project && dbt debug

dbt-seed:
	cd dbt_project && dbt seed

dbt-run:
	cd dbt_project && dbt run

dbt-test:
	cd dbt_project && dbt test

dbt-build:
	cd dbt_project && dbt build

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve --port 8080

# ==========================================
# DAGSTER
# ==========================================

dagster-up:
	cd dagster && docker-compose up -d --build
	@echo "🎯 Dagster UI: http://localhost:3000"

dagster-down:
	cd dagster && docker-compose down

dagster-logs:
	cd dagster && docker-compose logs -f

# ==========================================
# SODA
# ==========================================

soda-scan:
	cd soda && docker-compose run --rm soda

# ==========================================
# UTILITÁRIOS KAFKA
# ==========================================

redpanda-console:
	@echo "🌐 Redpanda Console: http://localhost:8080"
	@xdg-open http://localhost:8080 2>/dev/null || open http://localhost:8080 2>/dev/null || echo "Abra manualmente: http://localhost:8080"

kafka-topics:
	docker exec redpanda rpk topic list

kafka-consume:
	docker exec -it redpanda rpk topic consume ecommerce-events --brokers localhost:9092

kafka-describe:
	docker exec redpanda rpk topic describe ecommerce-events --brokers localhost:9092

# ==========================================
# BIGQUERY (via gcloud)
# ==========================================

bq-query:
	@read -p "Query SQL: " query; \
	gcloud bigquery query --use_legacy_sql=false "$$query"

bq-tables:
	gcloud bigquery tables list --dataset=bronze --project=$(GCP_PROJECT_ID)
	gcloud bigquery tables list --dataset=silver --project=$(GCP_PROJECT_ID)
	gcloud bigquery tables list --dataset=gold --project=$(GCP_PROJECT_ID)

# ==========================================
# SETUP INICIAL
# ==========================================

setup: .env
	@echo "🔧 Setup inicial..."
	@echo "1. Configure .env com suas credenciais GCP"
	@echo "2. Rode: make up"
	@echo "3. Aguarde Redpanda ficar healthy"
	@echo "4. Rode: make dbt-seed && make dbt-run && make dbt-test"

.env:
	cp .env.example .env
	@echo "📝 Arquivo .env criado. Edite com suas credenciais!"

# ==========================================
# CI/CD LOCAL (simula GitHub Actions)
# ==========================================

ci-lint:
	ruff check .
	ruff format --check .
	cd terraform && terraform fmt -check -recursive 2>/dev/null || echo "Terraform não configurado"

ci-test:
	cd dbt_project && dbt test

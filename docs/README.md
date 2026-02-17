# Online Shop — проект аналитики данных

Учебно-практический стенд: PostgreSQL → DBT → Airflow → Metabase.

## Цель проекта

Развернуть архитектуру инструментов и настроить демо онлайн-магазина с BI-визуализацией:

- **PostgreSQL** — хранение данных
- **Airflow** — оркестрация DAG (генерация данных, импорт CSV, запуск dbt)
- **DBT** — трансформации и витрины (CTE, staging, marts)
- **Metabase** — дашборды и визуализация

## Быстрый старт

### Требования

- Docker и Docker Compose
- Сеть `airflow-net` (создаётся при первом запуске или вручную: `docker network create airflow-net`)

### Запуск

```bash
# Создать сеть (если ещё не создана)
docker network create airflow-net

# Скопировать .env.example в .env и заполнить переменные
cp .env.example .env

# Запустить все сервисы
docker compose up -d

# Дождаться готовности (healthcheck)
docker compose ps
```

### Порты

| Сервис           | Порт | URL                    |
|------------------|------|------------------------|
| Airflow UI       | 8080 | http://localhost:8080 |
| Metabase         | 3000 | http://localhost:3000  |
| northwind-db     | 5433 | —                      |
| metabase-db      | 5434 | —                      |

## Переменные окружения

Основные переменные в `.env`:

| Переменная | Описание |
|------------|----------|
| `DB_USER` | Пользователь PostgreSQL |
| `DB_PASSWORD` | Пароль PostgreSQL |
| `DB_NAME` | Имя БД (например, northwind) |
| `DB_HOST` | Хост БД (northwind-db в Docker) |
| `DB_PORT` | Порт БД (5432 внутри сети) |
| `AIRFLOW_ADMIN_USER` | Логин админа Airflow |
| `AIRFLOW_ADMIN_PASSWORD` | Пароль админа Airflow |
| `FERNET_KEY` | Ключ шифрования Airflow |

## Структура проекта

```
├── dags/                    # Airflow DAG-и
│   ├── daily_generate_orders    # Генерация новых заказов (CSV)
│   ├── import_csv_to_postgres   # Импорт CSV в PostgreSQL
│   └── dbt_run_models          # dbt run + test + docs generate
├── dbt/                     # DBT проект
│   ├── models/
│   │   ├── staging/         # Staging-модели (views)
│   │   ├── marts/           # Витрины (fct_*)
│   │   └── sources/         # Описание источников
│   └── dbt_project.yml
├── plugins/                 # CSV-файлы (загружаются в БД)
├── init-db/                  # SQL-скрипты инициализации БД
├── docs/                     # Документация
│   ├── README.md            # Этот файл
│   ├── ARCHITECTURE.md      # Архитектура и диаграммы
│   ├── DATA_LINEAGE.md      # Data Lineage (dbt docs)
│   └── METABASE.md          # Настройка Metabase
└── docker-compose.yml
```

## DAG-и Airflow

| DAG | Расписание | Описание |
|-----|------------|----------|
| `daily_generate_orders` | Каждые 6 часов | Генерирует новые заказы в CSV |
| `import_csv_to_postgres` | Каждые 6 часов | Загружает CSV в PostgreSQL |
| `dbt_run_models` | Каждые 6 часов | Запускает dbt run, test, docs generate |

Цепочка: `daily_generate_orders` → `import_csv_to_postgres` → `dbt_run_models`.

## DBT

### Запуск вручную

```bash
# В контейнере dbt
docker compose exec dbt bash -c "cd /usr/app && dbt run --profiles-dir /usr/app"

# Отдельная модель
docker compose exec dbt bash -c "cd /usr/app && dbt run --select fct_order_facts --profiles-dir /usr/app"

# Полная пересборка incremental
docker compose exec dbt bash -c "cd /usr/app && dbt run --select fct_order_facts --full-refresh --profiles-dir /usr/app"

# Тесты
docker compose exec dbt bash -c "cd /usr/app && dbt test --profiles-dir /usr/app"

# Data Lineage
docker compose exec dbt bash -c "cd /usr/app && dbt docs generate --profiles-dir /usr/app"
docker compose exec dbt bash -c "cd /usr/app && dbt docs serve --profiles-dir /usr/app --port 8081"
```

### Слои моделей

- **Sources** — сырые таблицы (orders, order_items, customers, payment, products, reviews и др.)
- **Staging** — stg_orders, stg_order_items, stg_customers, stg_payments, stg_products, stg_reviews
- **Marts** — fct_* (витрины: revenue, AOV, LTV, retention, payment stats, reviews sentiment)

## Дашборд Metabase

Блоки метрик:

- **A. Продажи и конверсия** — Orders, AOV, Revenue, Items per Order
- **B. Клиенты** — New vs Returning, OPC, LTV, Retention
- **C. Продукты и категории** — Revenue по категориям, Items per SKU, Avg check
- **D. Платежи** — Revenue по способам оплаты, Payment conversion rate
- **E. Отзывы** — Avg rating, Positive/Negative sentiment

Подробнее: [docs/METABASE.md](METABASE.md)

## Документация

| Файл | Описание |
|------|----------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Архитектура, диаграммы потоков данных |
| [DATA_LINEAGE.md](DATA_LINEAGE.md) | Data Lineage, dbt docs |
| [METABASE.md](METABASE.md) | Настройка Metabase |

## Публичная ссылка

Дашборд (если развёрнут на сервере):

**http://185.68.21.193:3000/public/dashboard/9264be8d-6b76-4f0e-a60c-ac36e620892e**

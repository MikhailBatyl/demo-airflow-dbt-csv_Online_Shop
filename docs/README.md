# Online Shop — проект аналитики данных

Учебно-практический стенд: PostgreSQL → DBT → Airflow → Metabase.

## Цель проекта

Развернуть архитектуру инструментов и настроить демо онлайн-магазина с BI-визуализацией:

- **PostgreSQL** — хранение данных (northwind-db — бизнес-данные, metabase-db — метаданные Metabase)
- **Airflow** — оркестрация DAG (Celery executor + Redis), генерация данных, импорт CSV, запуск dbt
- **DBT** — трансформации и витрины (CTE, staging, marts)
- **Redis** — брокер сообщений для Celery executor
- **Metabase** — дашборды и визуализация

Опционально (`docker-compose_extra.yml`): **ClickHouse**, **MinIO**, **Jupyter + PySpark**.

## Быстрый старт

### Требования

- Docker и Docker Compose
- Сеть `airflow-net` (создаётся при первом запуске или вручную: `docker network create airflow-net`)

### Запуск

```bash
# Создать сеть (если ещё не создана)
docker network create airflow-net

# Заполнить переменные окружения в файле .env

# Запустить основные сервисы
docker compose up -d

# Дождаться готовности (healthcheck)
docker compose ps

# (Опционально) Запустить дополнительный стек (ClickHouse, MinIO, Jupyter)
docker compose -f docker-compose.yml -f docker-compose_extra.yml up -d
```

### Порты

| Сервис           | Порт  | URL / описание            |
|------------------|-------|---------------------------|
| Airflow UI       | 8081  | http://localhost:8081     |
| Metabase         | 3000  | http://localhost:3000     |
| northwind-db     | 5433  | PostgreSQL бизнес-данных  |
| metabase-db      | 5434  | PostgreSQL метаданных     |
| Redis            | 6379  | Брокер Celery (из `.env`) |

Дополнительный стек (`docker-compose_extra.yml`):

| Сервис           | Порт  | Описание                  |
|------------------|-------|---------------------------|
| ClickHouse HTTP  | 8123  | Аналитический движок      |
| ClickHouse Native| 9003  | Native-протокол           |
| MinIO S3 API     | 9002  | S3-совместимое хранилище  |
| MinIO Console    | 9001  | Web-консоль MinIO         |
| Jupyter Lab      | 8888  | PySpark notebooks         |

## Переменные окружения

Основные переменные в `.env`:

| Группа | Переменная | Описание |
|--------|------------|----------|
| **PostgreSQL** | `DB_USER` | Пользователь PostgreSQL |
| | `DB_PASSWORD` | Пароль PostgreSQL |
| | `DB_NAME` | Имя БД (northwind) |
| | `DB_HOST` | Хост БД (northwind-db в Docker) |
| | `DB_PORT` | Порт БД (5432 внутри сети) |
| **Redis** | `REDIS_HOST` | Хост Redis (redis) |
| | `REDIS_PORT` | Порт Redis (6379) |
| **Airflow** | `AIRFLOW_ADMIN_USER` | Логин админа Airflow |
| | `AIRFLOW_ADMIN_PASSWORD` | Пароль админа Airflow |
| | `AIRFLOW_ADMIN_FIRSTNAME` | Имя админа |
| | `AIRFLOW_ADMIN_LASTNAME` | Фамилия админа |
| | `AIRFLOW_ADMIN_ROLE` | Роль (Admin) |
| | `AIRFLOW_ADMIN_EMAIL` | Email админа |
| | `FERNET_KEY` | Ключ шифрования Airflow |
| | `AIRFLOW_EXECUTOR` | Тип executor (CeleryExecutor) |
| | `AIRFLOW_DAGS_ARE_PAUSED_AT_CREATION` | Пауза DAG при создании |
| | `AIRFLOW_LOAD_EXAMPLES` | Загрузка примеров DAG |
| | `AIRFLOW_AUTH_BACKEND` | Backend авторизации API |
| **Metabase DB** | `MB_DB_USER` | Пользователь БД Metabase |
| | `MB_DB_PASSWORD` | Пароль БД Metabase |
| | `MB_DB_NAME` | Имя БД метаданных Metabase |

> **Важно:** не коммитьте `.env` с реальными секретами в публичный репозиторий. Рекомендуется добавить `.env` в `.gitignore` и хранить шаблон `.env.example` с placeholder-значениями.

## Структура проекта

```
├── dags/                        # Airflow DAG-и
│   ├── generate_fake_shop_csv.py    # DAG: daily_generate_orders
│   ├── csv_to_postgres_dag.py       # DAG: import_csv_to_postgres
│   └── dbt_dag.py                   # DAG: dbt_run_models
├── dbt/                         # DBT-проект
│   ├── dbt_project.yml              # Конфигурация проекта
│   ├── profiles.yml                 # Профиль подключения
│   ├── .dbt/                        # Директория профилей (volume)
│   ├── models/
│   │   ├── sources/                 # Описание источников (sources.yml)
│   │   ├── staging/                 # Staging-модели (views)
│   │   │   ├── schema.yml
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_order_details.sql
│   │   │   ├── stg_order_count_year.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_payments.sql
│   │   │   ├── stg_products.sql
│   │   │   └── stg_reviews.sql
│   │   └── marts/                   # Витрины (tables, fct_*)
│   │       ├── schema.yml
│   │       ├── metrics.yml          # Семантические метрики (dbt Semantic Layer)
│   │       └── fct_*.sql            # 28 витринных моделей (см. ниже)
│   └── readme_dbt_stg.txt
├── plugins/                     # CSV-файлы (загружаются в БД)
│   ├── orders.csv
│   ├── order_items.csv
│   ├── customers.csv
│   ├── products.csv
│   ├── payment.csv
│   ├── reviews.csv
│   ├── suppliers.csv
│   └── shipments.csv
├── init-db/                     # SQL-скрипты инициализации БД
│   ├── 01-init.sql
│   └── 02-clickhouse-init.sql
├── docs/                        # Документация
│   ├── README.md                    # Этот файл
│   ├── ARCHITECTURE.md              # Архитектура и диаграммы
│   ├── DATA_LINEAGE.md              # Data Lineage (dbt docs)
│   ├── METABASE.md                  # Настройка Metabase
│   └── Script-5.sql                 # SQL-запросы (DBeaver)
├── Dockerfile                   # Образ Airflow (apache/airflow:2.9.3 + dbt + deps)
├── docker-compose.yml           # Основные сервисы
├── docker-compose_extra.yml     # Доп. сервисы (ClickHouse, MinIO, Jupyter)
├── requirements.txt             # Python-зависимости (dbt, psycopg2, pandas, ...)
├── .env                         # Переменные окружения (не коммитить с секретами!)
├── .gitignore
└── README.txt                   # Исходное описание проекта
```

## Сервисы Docker Compose

### Основной стек (`docker-compose.yml`)

| Сервис | Образ | Назначение |
|--------|-------|------------|
| `airflow-init` | Dockerfile (airflow:2.9.3) | Миграция БД, создание админа, настройка connections |
| `airflow-webserver` | Dockerfile | Web UI Airflow (порт 8081) |
| `airflow-scheduler` | Dockerfile | Планировщик DAG |
| `airflow-worker` | Dockerfile | Celery worker для выполнения задач |
| `redis` | redis:latest | Брокер сообщений для Celery |
| `northwind-db` | postgres:15 | PostgreSQL — бизнес-данные |
| `dbt` | dbt-postgres:1.7.9 | Контейнер для трансформаций dbt |
| `metabase-db` | postgres:15 | PostgreSQL — метаданные Metabase |
| `metabase` | metabase:latest | BI-платформа |

### Дополнительный стек (`docker-compose_extra.yml`)

| Сервис | Образ | Назначение |
|--------|-------|------------|
| `clickhouse` | clickhouse-server:23.8 | OLAP-движок |
| `minio` | minio:latest | Локальное S3-хранилище |
| `jupyter-pyspark` | jupyter/pyspark-notebook | Интерактивная аналитика |

Запуск с дополнительным стеком:

```bash
docker compose -f docker-compose.yml -f docker-compose_extra.yml up -d
```

## DAG-и Airflow

| DAG (dag_id) | Файл | Расписание | Описание |
|--------------|------|------------|----------|
| `daily_generate_orders` | `generate_fake_shop_csv.py` | `0 */6 * * *` (каждые 6 ч) | Генерирует новые заказы в CSV |
| `import_csv_to_postgres` | `csv_to_postgres_dag.py` | `0 */6 * * *` (каждые 6 ч) | Загружает CSV в PostgreSQL |
| `dbt_run_models` | `dbt_dag.py` | `0 */6 * * *` (каждые 6 ч) | Запускает dbt run, test, docs generate |

Цепочка зависимостей (через `ExternalTaskSensor`):

`daily_generate_orders` → `import_csv_to_postgres` → `dbt_run_models`

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

### Sources (сырые таблицы)

Источник `northwind` (схема `public`), описан в `dbt/models/sources/sources.yml`:

| Таблица | Описание |
|---------|----------|
| `orders` | Заказы клиентов |
| `order_items` | Позиции заказов |
| `customers` | Клиенты |
| `payment` | Платежи |
| `products` | Товары |
| `reviews` | Отзывы |
| `suppliers` | Поставщики |
| `shipments` | Отгрузки |

### Staging (views)

| Модель | Описание |
|--------|----------|
| `stg_orders` | Очищенные заказы |
| `stg_order_items` | Позиции заказов |
| `stg_order_details` | Детали заказов |
| `stg_order_count_year` | Подсчёт заказов по годам |
| `stg_customers` | Клиенты |
| `stg_payments` | Платежи |
| `stg_products` | Товары |
| `stg_reviews` | Отзывы |

### Marts (tables) — 28 витринных моделей

**Продажи и выручка:**
- `fct_order_facts` — основные факты по заказам
- `fct_orders_summary` — сводка заказов
- `fct_aov_revenue_summary` — AOV и выручка
- `fct_avg_aov_check` — средний AOV / средний чек
- `fct_revenue_by_month` — выручка по месяцам
- `fct_revenue_year_summary` — выручка по годам
- `fct_revenue_diff_time` — динамика выручки

**Заказы:**
- `fct_items_per_order` — товаров на заказ
- `fct_items_per_order_monthly` — товаров на заказ по месяцам
- `fct_order_count_year` — заказы по годам
- `fct_order_count_month_diff` — разница заказов по месяцам

**Клиенты:**
- `fct_customer_types` — типы клиентов (новые / повторные)
- `fct_ltv_by_customer` — LTV клиентов
- `fct_customer_retention` — retention
- `fct_customer_retention_cohort` — retention по когортам
- `fct_aov_orders_per_customer` — AOV и заказы на клиента
- `fct_orders_per_customer_monthly` — заказы на клиента по месяцам
- `fct_orders_per_customer_segmented` — заказы на клиента по сегментам

**Продукты и категории:**
- `fct_revenue_by_category` — выручка по категориям
- `fct_revenue_by_category_monthly` — выручка по категориям по месяцам
- `fct_items_per_order_by_category` — товаров на заказ по категориям
- `fct_items_per_sku` — товаров по SKU
- `fct_avg_check_by_category` — средний чек по категориям
- `fct_aov_by_category` — AOV по категориям
- `fct_aov_by_product` — AOV по продуктам
- `fct_product_price_stats` — ценовая статистика

**Платежи:**
- `fct_revenue_by_payment_method` — выручка по способам оплаты
- `fct_aov_by_payment_method` — AOV по способам оплаты
- `fct_payment_stats` — статистика платежей
- `fct_payment_conversion_rate` — конверсия оплаты

**Отзывы:**
- `fct_reviews_sentiment` — сентимент отзывов (positive / negative)

### Семантические метрики (metrics.yml)

Файл `dbt/models/marts/metrics.yml` определяет семантические модели и метрики для dbt Semantic Layer / MetricFlow:
- **fct_order_facts**: `order_count`, `total_revenue`, `total_items`, `avg_order_value`
- **fct_orders_summary**: `orders_count`, `total_sales`, `customer_count`

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
| [ARCHITECTURE.md](ARCHITECTURE.md) | Архитектура, диаграммы потоков данных, масштабирование |
| [DATA_LINEAGE.md](DATA_LINEAGE.md) | Data Lineage (dbt docs generate / serve) |
| [METABASE.md](METABASE.md) | Настройка Metabase, подключение к northwind-db |

## Публичная ссылка

Дашборд (если развёрнут на сервере):

**http://185.68.21.193:3000/public/dashboard/9264be8d-6b76-4f0e-a60c-ac36e620892e**

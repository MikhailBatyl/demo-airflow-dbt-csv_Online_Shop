# Архитектура проекта Online Shop

## Обзор

Проект реализует ETL/ELT-пайплайн для аналитики данных интернет-магазина:

```
CSV (plugins/) → PostgreSQL → DBT (analytics) → Metabase
       ↑              ↑              ↑
   Airflow DAG    Airflow DAG    Airflow DAG
```

## Диаграмма потоков данных

```mermaid
flowchart LR
    subgraph Generation
        GEN[daily_generate_orders]
        CSV[(CSV Files)]
    end

    subgraph Import
        IMP[import_csv_to_postgres]
        PG[(PostgreSQL\nnorthwind-db)]
    end

    subgraph Transform
        DBT[dbt_run_models]
        ANALYTICS[(analytics schema)]
    end

    subgraph BI
        MB[Metabase]
        DASH[Dashboards]
    end

    GEN -->|"Генерирует\nновые заказы"| CSV
    CSV -->|"Загружает\nв БД"| IMP
    IMP --> PG
    PG -->|"dbt run\ndbt test"| DBT
    DBT --> ANALYTICS
    ANALYTICS -->|"Подключение\nк northwind-db"| MB
    MB --> DASH
```

## Последовательность DAG-ов

```mermaid
sequenceDiagram
    participant A as Airflow
    participant G as daily_generate_orders
    participant I as import_csv_to_postgres
    participant D as dbt_run_models

    A->>G: Запуск (каждые 6 ч)
    G->>G: Генерация 10 новых заказов в CSV
    G->>I: Триггер (по расписанию)
    I->>I: Загрузка CSV в PostgreSQL
    I->>D: ExternalTaskSensor (ждёт success)
    D->>D: dbt run
    D->>D: dbt test
    D->>D: dbt docs generate
```

## Инфраструктура (Docker)

```mermaid
graph TB
    subgraph "Docker Network: airflow-net"
        subgraph "Airflow"
            WEBSERVER[airflow-webserver :8080]
            SCHEDULER[airflow-scheduler]
            WORKER[airflow-worker]
        end

        subgraph "Data"
            NORTHWIND[(northwind-db :5433)]
            METABASE_DB[(metabase-db :5434)]
        end

        subgraph "Tools"
            DBT[dbt : контейнер]
            REDIS[(redis)]
        end

        subgraph "BI"
            METABASE[metabase :3000]
        end
    end

    WEBSERVER --> SCHEDULER
    SCHEDULER --> WORKER
    WORKER --> REDIS
    WORKER --> NORTHWIND
    DBT --> NORTHWIND
    METABASE --> NORTHWIND
    METABASE --> METABASE_DB
```

## Схема данных

### Сырые таблицы (public)

| Таблица | Описание |
|---------|----------|
| orders | Заказы (order_id, order_date, customer_id, total_price) |
| order_items | Позиции заказов (order_item_id, order_id, product_id, quantity, price_at_purchase) |
| customers | Клиенты |
| products | Товары (product_id, product_name, category, price) |
| payment | Платежи (order_id, payment_method, amount, transaction_status) |
| shipments | Отгрузки |
| reviews | Отзывы (product_id, customer_id, rating, review_text, review_date) |
| suppliers | Поставщики |

### DBT: Sources → Staging → Marts

```mermaid
flowchart TB
    subgraph Sources["Sources (public)"]
        S_ORDERS[orders]
        S_ITEMS[order_items]
        S_CUST[customers]
        S_PAY[payment]
        S_PROD[products]
        S_REV[reviews]
    end

    subgraph Staging["Staging (views)"]
        STG_O[stg_orders]
        STG_I[stg_order_items]
        STG_D[stg_order_details]
        STG_C[stg_customers]
        STG_P[stg_payments]
        STG_PR[stg_products]
        STG_R[stg_reviews]
    end

    subgraph Marts["Marts (tables, analytics)"]
        FCT_ORD[fct_orders_summary]
        FCT_REV[fct_revenue_by_month]
        FCT_AOV[fct_aov_by_category]
        FCT_CUST[fct_customer_types]
        FCT_RET[fct_customer_retention]
        FCT_PAY[fct_payment_stats]
        FCT_REV_S[fct_reviews_sentiment]
        FCT_ORD_F[fct_order_facts<br/>incremental]
    end

    S_ORDERS --> STG_O
    S_ITEMS --> STG_I
    S_ITEMS --> STG_D
    S_CUST --> STG_C
    S_PAY --> STG_P
    S_PROD --> STG_PR
    S_REV --> STG_R

    STG_O --> FCT_ORD
    STG_O --> FCT_ORD_F
    STG_O --> FCT_REV
    STG_I --> FCT_AOV
    STG_C --> FCT_CUST
    STG_C --> FCT_RET
    STG_P --> FCT_PAY
    STG_R --> FCT_REV_S
```

## Incremental-модель

`fct_order_facts` — единственная incremental-модель:

- **Стратегия:** merge по `order_id`
- **Фильтр:** при инкрементальном запуске обрабатываются только заказы с `order_date >= max(order_date)` в целевой таблице
- **Использование:** ускорение загрузки при росте объёма данных

## Связи сервисов

| Откуда | Куда | Протокол/Порт |
|--------|------|---------------|
| Airflow | northwind-db | PostgreSQL 5432 |
| Airflow | redis | Redis 6379 |
| dbt | northwind-db | PostgreSQL 5432 |
| Metabase | northwind-db | PostgreSQL 5432 |
| Metabase | metabase-db | PostgreSQL 5432 |
| Пользователь | Airflow | HTTP 8080 |
| Пользователь | Metabase | HTTP 3000 |

## Рекомендации по масштабированию

- **Рост данных:** увеличить число incremental-моделей, партиционирование по дате
- **Частота запуска:** изменить `schedule_interval` в DAG
- **Мониторинг:** логи Airflow, алерты при падении DAG
- **CI/CD:** GitHub Actions для `dbt test`, `dbt build` при push

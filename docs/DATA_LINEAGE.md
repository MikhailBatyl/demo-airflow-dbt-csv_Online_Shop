# Data Lineage (dbt docs)

## Описание

Data Lineage — визуализация зависимостей между источниками данных, staging-моделями и витринами. Позволяет отслеживать поток данных от сырых таблиц до итоговых метрик.

## Генерация документации

### Через Airflow (автоматически)

DAG `dbt_run_models` после успешного `dbt run` и `dbt test` выполняет `dbt docs generate`. Документация обновляется каждые 6 часов.

### Вручную (локально или в контейнере)

```bash
# В контейнере dbt (с доступом к БД)
docker exec -it dbt bash -c "cd /usr/app && dbt docs generate --profiles-dir /usr/app"

# Локально (если установлен dbt и настроен profiles.yml)
cd dbt && dbt docs generate
```

Результат: файлы в `dbt/target/` (manifest.json, catalog.json, index.html).

## Просмотр документации

### Вариант 1: dbt docs serve (интерактивный)

```bash
# В контейнере
docker exec -it dbt bash -c "cd /usr/app && dbt docs serve --profiles-dir /usr/app --port 8081"

# Локально
cd dbt && dbt docs serve
```

Откройте http://localhost:8080 (или 8081) — интерактивный граф зависимостей, описание моделей, lineage.

### Вариант 2: Статические файлы

После `dbt docs generate` файлы находятся в `dbt/target/`:
- `index.html` — можно открыть в браузере (частичная функциональность без serve)
- `manifest.json`, `catalog.json` — метаданные для lineage

## Структура lineage

```
Sources (public)          Staging (views)           Marts (tables)
─────────────────         ─────────────────         ─────────────────
orders          ──────►   stg_orders         ──────► fct_orders_summary
order_items     ──────►   stg_order_items   ──────► fct_revenue_by_month
customers       ──────►   stg_customers     ──────► fct_customer_types
payment         ──────►   stg_payments      ──────► fct_payment_stats
products        ──────►   stg_products      ──────► fct_aov_by_category
reviews         ──────►   stg_reviews       ──────► fct_reviews_sentiment
...
```

## Ссылки

- [dbt docs](https://docs.getdbt.com/reference/commands/cmd-docs)
- [Data Lineage в dbt](https://docs.getdbt.com/docs/build/documentation)

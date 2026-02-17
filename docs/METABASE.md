# Настройка Metabase

## Архитектура

Metabase использует **две отдельные базы данных**:

| База данных | Назначение | Контейнер |
|-------------|------------|-----------|
| **metabase-db** | Метаданные Metabase (дашборды, вопросы, пользователи, настройки) | `metabase-db` |
| **northwind-db** | Бизнес-данные для визуализации (подключается в UI Metabase) | `northwind-db` |

Разделение необходимо для:
- изоляции метаданных BI от операционных данных;
- независимого бэкапа и масштабирования;
- избежания конфликтов при миграциях схемы данных.

## Переменные окружения

Добавьте в `.env` (опционально, есть значения по умолчанию):

```env
# Metabase — БД для метаданных (дашборды, настройки)
MB_DB_USER=metabase
MB_DB_PASSWORD=metabase
MB_DB_NAME=metabase
```

По умолчанию используются: `metabase` / `metabase` / `metabase`.

## Порты

| Сервис       | Порт | Описание                    |
|--------------|------|-----------------------------|
| Metabase UI  | 3000 | Веб-интерфейс              |
| metabase-db  | 5434 | PostgreSQL (внешний доступ) |
| northwind-db | 5433 | PostgreSQL (бизнес-данные)  |

## Первый запуск

1. Запустите стек:
   ```bash
   docker compose up -d
   ```

2. Дождитесь готовности Metabase (проверка healthcheck).

3. Откройте http://localhost:3000 (или ваш хост:3000).

4. Пройдите первоначальную настройку (админ, язык и т.п.).

5. Добавьте источник данных в Metabase:
   - **Database type:** PostgreSQL
   - **Host:** `northwind-db` (имя сервиса в Docker)
   - **Port:** 5432
   - **Database name:** значение из `${DB_NAME}` (например, `northwind`)
   - **Username / Password:** `${DB_USER}` / `${DB_PASSWORD}`

## Схема analytics (dbt)

Витрины dbt создаются в схеме `analytics`. В настройках подключения к `northwind-db` укажите:

- **Additional JDBC connection string options:** `currentSchema=analytics`  
  или выберите схему `analytics` при создании запросов.

## Бэкап метаданных Metabase

```bash
docker exec metabase-db pg_dump -U metabase metabase > metabase_backup.sql
```

## Миграция с общей БД

Если раньше Metabase использовал `northwind-db`:

1. Сделайте дамп метаданных из старой БД.
2. Разверните `metabase-db` и восстановите дамп.
3. Обновите переменные окружения Metabase и перезапустите контейнер.

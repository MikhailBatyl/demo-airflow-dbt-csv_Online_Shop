# Базовый образ Airflow
FROM apache/airflow:2.9.3

# Переключаемся на airflow пользователя
USER airflow

# Копируем requirements.txt внутрь контейнера
COPY requirements.txt /tmp/requirements.txt

# Обновляем pip и устанавливаем зависимости
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# Рабочая директория для проекта dbt
WORKDIR /usr/app

# Копируем проект dbt
COPY ./dbt /usr/app/dbt


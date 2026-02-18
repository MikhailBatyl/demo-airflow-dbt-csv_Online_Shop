from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import pandas as pd
import numpy as np
import random
from sqlalchemy import types as sqltypes

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,   # требует настройки SMTP в [smtp]
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

CSV_FOLDER = "/opt/airflow/plugins"

# -------------------------
# Вспомогательные функции
# -------------------------
def map_dtype_to_sqlalchemy(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return sqltypes.INTEGER()
    elif pd.api.types.is_float_dtype(dtype):
        return sqltypes.FLOAT()
    elif pd.api.types.is_bool_dtype(dtype):
        return sqltypes.BOOLEAN()
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return sqltypes.TIMESTAMP()
    else:
        return sqltypes.TEXT()

def read_csv_with_types(file_name):
    df = pd.read_csv(os.path.join(CSV_FOLDER, file_name))
    return df, df.dtypes.to_dict()

def fill_missing_values(df):
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            df[col] = df[col].fillna(1).astype(int)
        elif pd.api.types.is_float_dtype(dtype):
            df[col] = df[col].fillna(round(random.uniform(1, 500), 2)).astype(float)
        elif pd.api.types.is_bool_dtype(dtype):
            df[col] = df[col].fillna(False).astype(bool)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            df[col] = pd.to_datetime(df[col], errors='coerce').fillna(pd.Timestamp('2025-01-01'))
        else:
            df[col] = df[col].fillna("N/A").astype(str)
    return df

# Генерация новых данных
# -------------------------
@task
def generate_new_data():
    customers, _ = read_csv_with_types("customers.csv")
    orders, _ = read_csv_with_types("orders.csv")
    order_items, _ = read_csv_with_types("order_items.csv")
    payments, _ = read_csv_with_types("payment.csv")
    shipments, _ = read_csv_with_types("shipments.csv")

    # Убираем столбец order_amount, если есть
    if "order_amount" in orders.columns:
        orders = orders.drop(columns=["order_amount"])

    # Берём только существующие колонки для новых записей
    existing_order_cols = orders.columns.tolist()
    existing_item_cols = order_items.columns.tolist()
    existing_payment_cols = payments.columns.tolist()
    existing_shipment_cols = shipments.columns.tolist()

    last_order_id = int(orders["order_id"].max() if len(orders) > 0 else 0)
    last_item_id = int(order_items["order_item_id"].max() if len(order_items) > 0 else 0)
    last_payment_id = int(payments["payment_id"].max() if len(payments) > 0 else 0)
    last_shipment_id = int(shipments["shipment_id"].max() if len(shipments) > 0 else 0)

    new_orders, new_items, new_payments, new_shipments = [], [], [], []
    product_ids = [pid for pid in order_items["product_id"].dropna().unique() if not pd.isna(pid)]
    if not product_ids:
        product_ids = list(range(1, 101))

    for i in range(10):
        new_order_id = last_order_id + i + 1

        # --- равномерное распределение по кварталам ---
        quarter = random.randint(1, 4)
        if quarter == 1:
            start_date, end_date = pd.Timestamp("2025-01-01"), pd.Timestamp("2025-03-31")
        elif quarter == 2:
            start_date, end_date = pd.Timestamp("2025-04-01"), pd.Timestamp("2025-06-30")
        elif quarter == 3:
            start_date, end_date = pd.Timestamp("2025-07-01"), pd.Timestamp("2025-09-30")
        else:
            start_date, end_date = pd.Timestamp("2025-10-01"), pd.Timestamp("2025-12-31")

        days_range = (end_date - start_date).days
        new_order_date = start_date + pd.Timedelta(days=random.randint(0, days_range))

        # Новая запись только с существующими колонками
        new_order = {col: orders[col].dropna().sample(1).iloc[0] for col in existing_order_cols}
        new_order["order_id"] = new_order_id
        if "order_date" in new_order:
            new_order["order_date"] = new_order_date.date()
        new_orders.append(new_order)

        for _ in range(random.randint(1, 3)):
            new_item = {col: order_items[col].dropna().sample(1).iloc[0] for col in existing_item_cols}
            new_item["order_item_id"] = last_item_id + len(new_items) + 1
            new_item["order_id"] = new_order_id
            new_items.append(new_item)

        new_payment = {col: payments[col].dropna().sample(1).iloc[0] for col in existing_payment_cols}
        new_payment["payment_id"] = last_payment_id + len(new_payments) + 1
        new_payment["order_id"] = new_order_id
        if "payment_date" in new_payment:
            new_payment["payment_date"] = new_order_date.date()
        new_payments.append(new_payment)

        new_shipment = {col: shipments[col].dropna().sample(1).iloc[0] for col in existing_shipment_cols}
        new_shipment["shipment_id"] = last_shipment_id + len(new_shipments) + 1
        new_shipment["order_id"] = new_order_id
        if "shipment_date" in new_shipment:
            new_shipment["shipment_date"] = (new_order_date + pd.Timedelta(days=random.randint(1, 5))).date()
        new_shipments.append(new_shipment)

    # Конкатенация и заполнение NaN
    orders = fill_missing_values(pd.concat([orders, pd.DataFrame(new_orders)], ignore_index=True))
    order_items = fill_missing_values(pd.concat([order_items, pd.DataFrame(new_items)], ignore_index=True))
    payments = fill_missing_values(pd.concat([payments, pd.DataFrame(new_payments)], ignore_index=True))
    shipments = fill_missing_values(pd.concat([shipments, pd.DataFrame(new_shipments)], ignore_index=True))

    # Сохраняем CSV
    orders.to_csv(os.path.join(CSV_FOLDER, "orders.csv"), index=False, date_format="%Y-%m-%d")
    order_items.to_csv(os.path.join(CSV_FOLDER, "order_items.csv"), index=False)
    payments.to_csv(os.path.join(CSV_FOLDER, "payment.csv"), index=False, date_format="%Y-%m-%d")
    shipments.to_csv(os.path.join(CSV_FOLDER, "shipments.csv"), index=False, date_format="%Y-%m-%d")

    print("✅ Новые заказы сгенерированы корректно, новые столбцы не добавлены.")
    
# -------------------------
# DAG
# -------------------------
with DAG(
    dag_id="daily_generate_orders",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["generate", "orders", "daily"]
) as dag:

    generate_task = generate_new_data()


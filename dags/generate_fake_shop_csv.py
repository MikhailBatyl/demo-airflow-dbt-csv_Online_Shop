from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import numpy as np
import random
from sqlalchemy import types as sqltypes

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

# -------------------------
# Генерация новых данных
# -------------------------
# def generate_new_data():
#     customers, _ = read_csv_with_types("customers.csv")
#     orders, _ = read_csv_with_types("orders.csv")
#     order_items, _ = read_csv_with_types("order_items.csv")
#     payments, _ = read_csv_with_types("payment.csv")
#     shipments, _ = read_csv_with_types("shipments.csv")

#     # Убираем столбец order_amount, если есть
#     if "order_amount" in orders.columns:
#         orders = orders.drop(columns=["order_amount"])

#     # Берём только существующие колонки для новых записей
#     existing_order_cols = orders.columns.tolist()
#     existing_item_cols = order_items.columns.tolist()
#     existing_payment_cols = payments.columns.tolist()
#     existing_shipment_cols = shipments.columns.tolist()

#     last_order_id = int(orders["order_id"].max() if len(orders) > 0 else 0)
#     last_item_id = int(order_items["order_item_id"].max() if len(order_items) > 0 else 0)
#     last_payment_id = int(payments["payment_id"].max() if len(payments) > 0 else 0)
#     last_shipment_id = int(shipments["shipment_id"].max() if len(shipments) > 0 else 0)
#     last_order_date = pd.to_datetime(orders["order_date"].max()) if len(orders) > 0 else pd.to_datetime("2025-01-01")

#     new_orders, new_items, new_payments, new_shipments = [], [], [], []
#     product_ids = [pid for pid in order_items["product_id"].dropna().unique() if not pd.isna(pid)]
#     if not product_ids:
#         product_ids = list(range(1, 101))

#     for i in range(10):
#         new_order_id = last_order_id + i + 1
#         new_order_date = last_order_date + pd.Timedelta(days=i + 1)

#         # Новая запись только с существующими колонками
#         new_order = {col: orders[col].dropna().sample(1).iloc[0] for col in existing_order_cols}
#         new_order["order_id"] = new_order_id
#         if "order_date" in new_order:
#             new_order["order_date"] = new_order_date.date()
#         new_orders.append(new_order)

#         for _ in range(random.randint(1, 3)):
#             new_item = {col: order_items[col].dropna().sample(1).iloc[0] for col in existing_item_cols}
#             new_item["order_item_id"] = last_item_id + len(new_items) + 1
#             new_item["order_id"] = new_order_id
#             new_items.append(new_item)

#         new_payment = {col: payments[col].dropna().sample(1).iloc[0] for col in existing_payment_cols}
#         new_payment["payment_id"] = last_payment_id + len(new_payments) + 1
#         new_payment["order_id"] = new_order_id
#         if "payment_date" in new_payment:
#             new_payment["payment_date"] = new_order_date.date()
#         new_payments.append(new_payment)

#         new_shipment = {col: shipments[col].dropna().sample(1).iloc[0] for col in existing_shipment_cols}
#         new_shipment["shipment_id"] = last_shipment_id + len(new_shipments) + 1
#         new_shipment["order_id"] = new_order_id
#         if "shipment_date" in new_shipment:
#             new_shipment["shipment_date"] = (new_order_date + pd.Timedelta(days=random.randint(1, 5))).date()
#         new_shipments.append(new_shipment)

#     # Конкатенация и заполнение NaN
#     orders = fill_missing_values(pd.concat([orders, pd.DataFrame(new_orders)], ignore_index=True))
#     order_items = fill_missing_values(pd.concat([order_items, pd.DataFrame(new_items)], ignore_index=True))
#     payments = fill_missing_values(pd.concat([payments, pd.DataFrame(new_payments)], ignore_index=True))
#     shipments = fill_missing_values(pd.concat([shipments, pd.DataFrame(new_shipments)], ignore_index=True))

#     # Сохраняем CSV
#     orders.to_csv(os.path.join(CSV_FOLDER, "orders.csv"), index=False, date_format="%Y-%m-%d")
#     order_items.to_csv(os.path.join(CSV_FOLDER, "order_items.csv"), index=False)
#     payments.to_csv(os.path.join(CSV_FOLDER, "payment.csv"), index=False, date_format="%Y-%m-%d")
#     shipments.to_csv(os.path.join(CSV_FOLDER, "shipments.csv"), index=False, date_format="%Y-%m-%d")

#     print("✅ Новые заказы сгенерированы корректно, новые столбцы не добавлены.")

#--------------------------

# Генерация новых данных
# -------------------------
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
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    tags=["generate", "orders", "daily"]
) as dag:

    generate_task = PythonOperator(
        task_id="generate_new_data",
        python_callable=generate_new_data
    )

    generate_task




#-----------------------------------------------------------------------------------------------------------------

# Вар 2 

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import os
# import pandas as pd
# import numpy as np
# import random
# from sqlalchemy import types as sqltypes

# CSV_FOLDER = "/opt/airflow/plugins"

# # -------------------------
# # Вспомогательные функции
# # -------------------------
# def map_dtype_to_sqlalchemy(dtype):
#     if pd.api.types.is_integer_dtype(dtype):
#         return sqltypes.INTEGER()
#     elif pd.api.types.is_float_dtype(dtype):
#         return sqltypes.FLOAT()
#     elif pd.api.types.is_bool_dtype(dtype):
#         return sqltypes.BOOLEAN()
#     elif pd.api.types.is_datetime64_any_dtype(dtype):
#         return sqltypes.TIMESTAMP()
#     else:
#         return sqltypes.TEXT()

# def read_csv_with_types(file_name):
#     df = pd.read_csv(os.path.join(CSV_FOLDER, file_name))
#     return df, df.dtypes.to_dict()

# def fill_missing_values(df):
#     for col in df.columns:
#         dtype = df[col].dtype
#         if pd.api.types.is_integer_dtype(dtype):
#             df[col] = df[col].fillna(1).astype(int)
#         elif pd.api.types.is_float_dtype(dtype):
#             df[col] = df[col].fillna(round(random.uniform(1, 500), 2)).astype(float)
#         elif pd.api.types.is_bool_dtype(dtype):
#             df[col] = df[col].fillna(False).astype(bool)
#         elif pd.api.types.is_datetime64_any_dtype(dtype):
#             df[col] = pd.to_datetime(df[col], errors='coerce').fillna(pd.Timestamp('2025-01-01'))
#         else:
#             df[col] = df[col].fillna("N/A").astype(str)
#     return df

# # -------------------------
# # Генерация новых данных
# # -------------------------
# def generate_new_data():
#     customers, _ = read_csv_with_types("customers.csv")
#     orders, _ = read_csv_with_types("orders.csv")
#     order_items, _ = read_csv_with_types("order_items.csv")
#     payments, _ = read_csv_with_types("payment.csv")
#     shipments, _ = read_csv_with_types("shipments.csv")

#     # Убираем столбец order_amount, если есть
#     if "order_amount" in orders.columns:
#         orders = orders.drop(columns=["order_amount"])

#     # Добавляем недостающие колонки
#     if "status" not in orders.columns:
#         orders["status"] = [random.choice(["pending", "shipped", "delivered"]) for _ in range(len(orders))]

#     last_order_id = int(orders["order_id"].max() if len(orders) > 0 else 0)
#     last_item_id = int(order_items["order_item_id"].max() if len(order_items) > 0 else 0)
#     last_payment_id = int(payments["payment_id"].max() if len(payments) > 0 else 0)
#     last_shipment_id = int(shipments["shipment_id"].max() if len(shipments) > 0 else 0)
#     last_order_date = pd.to_datetime(orders["order_date"].max()) if len(orders) > 0 else pd.to_datetime("2025-01-01")

#     new_orders, new_items, new_payments, new_shipments = [], [], [], []
#     product_ids = [pid for pid in order_items["product_id"].dropna().unique() if not pd.isna(pid)]
#     if not product_ids:
#         product_ids = list(range(1, 101))

#     for i in range(10):
#         new_order_id = last_order_id + i + 1
#         new_order_date = last_order_date + pd.Timedelta(days=i + 1)

#         new_order = {
#             "order_id": new_order_id,
#             "customer_id": int(customers.sample(1).iloc[0]["customer_id"]),
#             "order_date": new_order_date.date(),
#             "status": random.choice(["pending", "shipped", "delivered"])
#         }

#         num_items = random.randint(1, 3)
#         for _ in range(num_items):
#             quantity = random.randint(1, 5)
#             price = round(random.uniform(10, 500), 2)
#             new_items.append({
#                 "order_item_id": last_item_id + len(new_items) + 1,
#                 "order_id": new_order_id,
#                 "product_id": int(random.choice(product_ids)),
#                 "quantity": int(quantity),
#                 "price": float(price)
#             })

#         new_orders.append(new_order)

#         new_payments.append({
#             "payment_id": last_payment_id + len(new_payments) + 1,
#             "order_id": new_order_id,
#             "amount": round(random.uniform(10, 5000), 2),  # Генерируем случайный ненулевой платеж
#             "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
#             "payment_status": random.choice(["Completed", "Pending", "Failed"]),
#             "payment_date": new_order_date.date()
#         })

#         new_shipments.append({
#             "shipment_id": last_shipment_id + len(new_shipments) + 1,
#             "order_id": new_order_id,
#             "shipment_date": (new_order_date + pd.Timedelta(days=random.randint(1, 5))).date(),
#             "carrier": random.choice(["DHL", "FedEx", "UPS", "RussianPost"]),
#             "tracking_number": f"TRK{random.randint(100000, 999999)}",
#             "status": random.choice(["preparing", "in_transit", "delivered"])
#         })

#     # Конкатенация и заполнение NaN
#     orders = fill_missing_values(pd.concat([orders, pd.DataFrame(new_orders)], ignore_index=True))
#     order_items = fill_missing_values(pd.concat([order_items, pd.DataFrame(new_items)], ignore_index=True))
#     payments = fill_missing_values(pd.concat([payments, pd.DataFrame(new_payments)], ignore_index=True))
#     shipments = fill_missing_values(pd.concat([shipments, pd.DataFrame(new_shipments)], ignore_index=True))

#     # Сохраняем CSV
#     orders.to_csv(os.path.join(CSV_FOLDER, "orders.csv"), index=False, date_format="%Y-%m-%d")
#     order_items.to_csv(os.path.join(CSV_FOLDER, "order_items.csv"), index=False)
#     payments.to_csv(os.path.join(CSV_FOLDER, "payment.csv"), index=False, date_format="%Y-%m-%d")
#     shipments.to_csv(os.path.join(CSV_FOLDER, "shipments.csv"), index=False, date_format="%Y-%m-%d")

#     print("✅ Новые заказы сгенерированы корректно, столбец order_amount удалён, 0.0 отсутствуют.")

# # -------------------------
# # DAG
# # -------------------------
# with DAG(
#     dag_id="daily_generate_orders",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="0 */6 * * *",
#     catchup=False,
#     tags=["generate", "orders", "daily"]
# ) as dag:

#     generate_task = PythonOperator(
#         task_id="generate_new_data",
#         python_callable=generate_new_data
#     )

#     generate_task
    
    
    
# -------------------------------------------------------------------------------------------------------------------------
    
# Вар 1

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook
# from datetime import datetime
# import os
# import pandas as pd
# import numpy as np
# import random
# from sqlalchemy import types as sqltypes

# # =========================
# # 1. Конфигурация
# # =========================
# CSV_FOLDER = "/opt/airflow/plugins"
# TABLES = ["customers", "orders", "order_items", "payment", "suppliers", "products", "shipments", "reviews"]

# # =========================
# # 2. Вспомогательные функции
# # =========================
# def map_dtype_to_sqlalchemy(dtype):
#     if pd.api.types.is_integer_dtype(dtype):
#         return sqltypes.INTEGER()
#     elif pd.api.types.is_float_dtype(dtype):
#         return sqltypes.FLOAT()
#     elif pd.api.types.is_bool_dtype(dtype):
#         return sqltypes.BOOLEAN()
#     elif pd.api.types.is_datetime64_any_dtype(dtype):
#         return sqltypes.TIMESTAMP()
#     else:
#         return sqltypes.TEXT()

# def read_csv_with_types(file_name):
#     file_path = os.path.join(CSV_FOLDER, file_name)
#     df = pd.read_csv(file_path)
#     return df, df.dtypes.to_dict()

# def cast_value_to_dtype(value, dtype):
#     if pd.api.types.is_integer_dtype(dtype):
#         return int(value)
#     elif pd.api.types.is_float_dtype(dtype):
#         return float(value)
#     elif pd.api.types.is_bool_dtype(dtype):
#         return bool(value)
#     elif pd.api.types.is_datetime64_any_dtype(dtype):
#         return pd.to_datetime(value)
#     else:
#         return str(value)

# # =========================
# # 3. Генерация новых данных
# # =========================
# # def generate_new_data():
# #     customers, customers_types = read_csv_with_types("customers.csv")
# #     orders, orders_types = read_csv_with_types("orders.csv")
# #     order_items, order_items_types = read_csv_with_types("order_items.csv")
# #     payments, payments_types = read_csv_with_types("payment.csv")
# #     shipments, shipments_types = read_csv_with_types("shipments.csv")

# #     # Добавляем недостающие колонки в orders
# #     if "status" not in orders.columns:
# #         orders["status"] = [random.choice(["pending", "shipped", "delivered"]) for _ in range(len(orders))]
# #         orders_types["status"] = object

# #     if "order_amount" not in orders.columns:
# #         orders["order_amount"] = 0.0
# #         orders_types["order_amount"] = float

# #     # Начальные id
# #     last_order_id = int(orders["order_id"].max() if len(orders) > 0 else 0)
# #     last_item_id = int(order_items["order_item_id"].max() if len(order_items) > 0 else 0)
# #     last_payment_id = int(payments["payment_id"].max() if len(payments) > 0 else 0)
# #     last_shipment_id = int(shipments["shipment_id"].max() if len(shipments) > 0 else 0)

# #     # Последняя дата заказа
# #     last_order_date = pd.to_datetime(orders["order_date"].max()) if len(orders) > 0 else pd.to_datetime("2025-01-01")

# #     new_orders, new_items, new_payments, new_shipments = [], [], [], []

# #     product_ids = [pid for pid in order_items["product_id"].dropna().unique() if not pd.isna(pid)]
# #     if len(product_ids) == 0:
# #         product_ids = list(range(1, 101))  # 🔥 fallback, если вдруг нет товаров

# #     for i in range(10):  # 🔟 новых заказов
# #         new_order_id = last_order_id + i + 1
# #         new_order_date = last_order_date + pd.Timedelta(days=i + 1)

# #         # Новый заказ
# #         new_order = {
# #             "order_id": new_order_id,
# #             "customer_id": int(customers.sample(1).iloc[0]["customer_id"]),
# #             "order_date": new_order_date.strftime("%Y-%m-%d"),
# #             "status": random.choice(["pending", "shipped", "delivered"]),
# #             "order_amount": 0.0
# #         }

# #         # Order items (от 1 до 3 позиций)
# #         num_items = random.randint(1, 3)
# #         total_amount = 0.0
# #         for j in range(num_items):
# #             quantity = random.randint(1, 5)
# #             price = round(random.uniform(10, 500), 2)
# #             total_amount += quantity * price
# #             new_item = {
# #                 "order_item_id": last_item_id + len(new_items) + 1,
# #                 "order_id": new_order_id,
# #                 "product_id": int(random.choice(product_ids)),
# #                 "quantity": int(quantity),
# #                 "price": float(price)
# #             }
# #             new_items.append(new_item)

# #         # Обновляем сумму заказа
# #         new_order["order_amount"] = round(total_amount, 2)
# #         new_orders.append(new_order)

# #         # Payment
# #         new_payment = {
# #             "payment_id": last_payment_id + len(new_payments) + 1,
# #             "order_id": new_order_id,
# #             "amount": round(total_amount, 2),
# #             "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
# #             "payment_status": random.choice(["Completed", "Pending", "Failed"]),
# #             "payment_date": new_order_date.strftime("%Y-%m-%d")
# #         }
# #         new_payments.append(new_payment)

# #         # Shipment
# #         new_shipment = {
# #             "shipment_id": last_shipment_id + len(new_shipments) + 1,
# #             "order_id": new_order_id,
# #             "shipment_date": (new_order_date + pd.Timedelta(days=random.randint(1, 5))).strftime("%Y-%m-%d"),
# #             "carrier": random.choice(["DHL", "FedEx", "UPS", "RussianPost"]),
# #             "tracking_number": f"TRK{random.randint(100000, 999999)}",
# #             "status": random.choice(["preparing", "in_transit", "delivered"])
# #         }
# #         new_shipments.append(new_shipment)

# #     # Конкатенация с существующими таблицами
# #     orders = pd.concat([orders, pd.DataFrame(new_orders)], ignore_index=True)
# #     order_items = pd.concat([order_items, pd.DataFrame(new_items)], ignore_index=True)
# #     payments = pd.concat([payments, pd.DataFrame(new_payments)], ignore_index=True)
# #     shipments = pd.concat([shipments, pd.DataFrame(new_shipments)], ignore_index=True)

# #     # Заполняем NaN дефолтами
# #     orders.fillna({"order_amount": 0.0, "status": "pending"}, inplace=True)
# #     order_items.fillna({"price": 0.0, "quantity": 1}, inplace=True)
# #     payments.fillna({"amount": 0.0, "payment_status": "Pending"}, inplace=True)
# #     shipments.fillna({"status": "preparing"}, inplace=True)

# #     # Сохраняем все изменения
# #     orders.to_csv(os.path.join(CSV_FOLDER, "orders.csv"), index=False)
# #     order_items.to_csv(os.path.join(CSV_FOLDER, "order_items.csv"), index=False)
# #     payments.to_csv(os.path.join(CSV_FOLDER, "payment.csv"), index=False)
# #     shipments.to_csv(os.path.join(CSV_FOLDER, "shipments.csv"), index=False)

# #     print("✅ Добавлено 10 новых заказов вместе с order_items, payment и shipments")

# def generate_new_data():
#     customers, _ = read_csv_with_types("customers.csv")
#     orders, _ = read_csv_with_types("orders.csv")
#     order_items, _ = read_csv_with_types("order_items.csv")
#     payments, _ = read_csv_with_types("payment.csv")
#     shipments, _ = read_csv_with_types("shipments.csv")

#     # Добавляем недостающие колонки в orders
#     if "status" not in orders.columns:
#         orders["status"] = [random.choice(["pending", "shipped", "delivered"]) for _ in range(len(orders))]
#     if "order_amount" not in orders.columns:
#         orders["order_amount"] = 0.0

#     # Последние id
#     last_order_id = int(orders["order_id"].max() if len(orders) > 0 else 0)
#     last_item_id = int(order_items["order_item_id"].max() if len(order_items) > 0 else 0)
#     last_payment_id = int(payments["payment_id"].max() if len(payments) > 0 else 0)
#     last_shipment_id = int(shipments["shipment_id"].max() if len(shipments) > 0 else 0)

#     last_order_date = pd.to_datetime(orders["order_date"].max()) if len(orders) > 0 else pd.to_datetime("2025-01-01")

#     new_orders, new_items, new_payments, new_shipments = [], [], [], []

#     product_ids = [pid for pid in order_items["product_id"].dropna().unique() if not pd.isna(pid)]
#     if not product_ids:
#         product_ids = list(range(1, 101))  # fallback

#     for i in range(10):  # 10 новых заказов
#         new_order_id = last_order_id + i + 1
#         new_order_date = last_order_date + pd.Timedelta(days=i + 1)

#         # Новый заказ
#         new_order = {
#             "order_id": new_order_id,
#             "customer_id": int(customers.sample(1).iloc[0]["customer_id"]),
#             "order_date": new_order_date.date(),  # 👈 DATE
#             "status": random.choice(["pending", "shipped", "delivered"]),
#             "order_amount": 0.0
#         }

#         # Order items (1–3 позиции)
#         num_items = random.randint(1, 3)
#         total_amount = 0.0
#         for j in range(num_items):
#             quantity = random.randint(1, 5)
#             price = round(random.uniform(10, 500), 2)
#             total_amount += quantity * price
#             new_item = {
#                 "order_item_id": last_item_id + len(new_items) + 1,
#                 "order_id": new_order_id,
#                 "product_id": int(random.choice(product_ids)),
#                 "quantity": int(quantity),
#                 "price": float(price)
#             }
#             new_items.append(new_item)

#         new_order["order_amount"] = round(total_amount, 2)
#         new_orders.append(new_order)

#         # Payment
#         new_payment = {
#             "payment_id": last_payment_id + len(new_payments) + 1,
#             "order_id": new_order_id,
#             "amount": round(total_amount, 2),
#             "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
#             "payment_status": random.choice(["Completed", "Pending", "Failed"]),
#             "payment_date": new_order_date.date()  # 👈 DATE
#         }
#         new_payments.append(new_payment)

#         # Shipment
#         new_shipment = {
#             "shipment_id": last_shipment_id + len(new_shipments) + 1,
#             "order_id": new_order_id,
#             "shipment_date": (new_order_date + pd.Timedelta(days=random.randint(1, 5))).date(),  # 👈 DATE
#             "carrier": random.choice(["DHL", "FedEx", "UPS", "RussianPost"]),
#             "tracking_number": f"TRK{random.randint(100000, 999999)}",
#             "status": random.choice(["preparing", "in_transit", "delivered"])
#         }
#         new_shipments.append(new_shipment)

#     # Конкатенация с существующими таблицами
#     orders = pd.concat([orders, pd.DataFrame(new_orders)], ignore_index=True)
#     order_items = pd.concat([order_items, pd.DataFrame(new_items)], ignore_index=True)
#     payments = pd.concat([payments, pd.DataFrame(new_payments)], ignore_index=True)
#     shipments = pd.concat([shipments, pd.DataFrame(new_shipments)], ignore_index=True)

#     # Заполняем NaN дефолтами
#     orders.fillna({"order_amount": 0.0, "status": "pending"}, inplace=True)
#     order_items.fillna({"price": 0.0, "quantity": 1}, inplace=True)
#     payments.fillna({"amount": 0.0, "payment_status": "Pending"}, inplace=True)
#     shipments.fillna({"status": "preparing"}, inplace=True)

#     # Сохраняем CSV
#     orders.to_csv(os.path.join(CSV_FOLDER, "orders.csv"), index=False, date_format="%Y-%m-%d")
#     order_items.to_csv(os.path.join(CSV_FOLDER, "order_items.csv"), index=False)
#     payments.to_csv(os.path.join(CSV_FOLDER, "payment.csv"), index=False, date_format="%Y-%m-%d")
#     shipments.to_csv(os.path.join(CSV_FOLDER, "shipments.csv"), index=False, date_format="%Y-%m-%d")

#     print("✅ Добавлено 10 новых заказов с корректными DATE и числовыми значениями")

# # =========================
# # 4. Загрузка CSV в PostgreSQL с генерацией случайных значений для NULL
# # =========================
# # def load_csv_to_postgres():
# #     hook = PostgresHook(postgres_conn_id="postgres_conn")
# #     engine = hook.get_sqlalchemy_engine()
# #     chunksize = 400

# #     for file_name in os.listdir(CSV_FOLDER):
# #         if not file_name.endswith(".csv"):
# #             continue

# #         table_name = file_name.replace(".csv", "").lower()
# #         file_path = os.path.join(CSV_FOLDER, file_name)

# #         # Определяем столбец ID
# #         id_col = None
# #         if "order_id" in file_name:
# #             id_col = "order_id"
# #         elif "order_item_id" in file_name:
# #             id_col = "order_item_id"
# #         elif "payment_id" in file_name:
# #             id_col = "payment_id"

# #         sample_df = pd.read_csv(file_path, nrows=5)
# #         dtype_mapping = {col: map_dtype_to_sqlalchemy(dtype) for col, dtype in sample_df.dtypes.items()}
        
# #         total_inserted = 0  # счётчик для таблицы

# #         # Читаем CSV порциями
# #         for chunk in pd.read_csv(file_path, chunksize=chunksize):
# #             if id_col:
# #                 # Получаем существующие ID из базы
# #                 existing_ids = pd.read_sql(f"SELECT {id_col} FROM {table_name}", engine)
# #                 chunk = chunk[~chunk[id_col].isin(existing_ids[id_col])]

# #             if chunk.empty:
# #                 continue

# #             chunk.to_sql(table_name, engine, if_exists="append", index=False, dtype=dtype_mapping)
# #             total_inserted += len(chunk)

# #         print(f"✅ Загружено {total_inserted} строк в таблицу {table_name}")

# #             # Заполняем NULL случайными значениями
# #         for col in chunk.columns:
# #             if chunk[col].isnull().all():
# #                 dtype = chunk[col].dtype
# #                 if np.issubdtype(dtype, np.integer):
# #                     chunk[col] = np.random.randint(1, 100, size=len(chunk))
# #                 elif np.issubdtype(dtype, np.floating):
# #                     chunk[col] = np.random.uniform(1.0, 1000.0, size=len(chunk))
# #                 elif np.issubdtype(dtype, np.object_):
# #                     chunk[col] = np.random.choice(['A', 'B', 'C', 'D'], size=len(chunk))
# #                 elif np.issubdtype(dtype, np.datetime64):
# #                     chunk[col] = pd.to_datetime('2025-01-01') + pd.to_timedelta(
# #                         np.random.randint(0, 365, size=len(chunk)), unit='D'
# #                     )

# #             chunk.to_sql(table_name, engine, if_exists="append", index=False, dtype=dtype_mapping)
# #             print(f"✅ Загружено {len(chunk)} новых строк в таблицу {table_name}")

# # =========================
# # 5. DAG
# # =========================
# with DAG(
#     dag_id="daily_generate_orders",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="0 */6 * * *",  # раз в 6 часов
#     catchup=False,
#     tags=["generate", "orders", "daily"]
# ) as dag:

#     generate_task = PythonOperator(
#         task_id="generate_new_data",
#         python_callable=generate_new_data
#     )

#     # load_task = PythonOperator(
#     #     task_id="load_csv_to_postgres",
#     #     python_callable=load_csv_to_postgres
#     # )

#     generate_task
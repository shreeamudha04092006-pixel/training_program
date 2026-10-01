import re
import pandas as pd
import json
import numpy as np
from collections import OrderedDict


# 1. Robust Log Record Parser

def parse_numeric_log(log_records):
    valid = []
    corrupted_count = 0

    for record in log_records:
        try:
            parts = record.split(",")
            transaction_id = parts[0].strip()
            value = float(parts[1].strip())

            valid.append({
                "id": transaction_id,
                "value": value
            })

        except:
            corrupted_count += 1

    return {
        "valid": valid,
        "corrupted_count": corrupted_count
    }


log_records = [
    "TXN101, 145.50",
    "TXN102, invalid_num",
    "TXN103, 300.00",
    "CORRUPTED_LINE",
    "TXN104, 82.25"
]

print("PROGRAM 1")
print(parse_numeric_log(log_records))
print()


# 2. Memory-Efficient Batch Stream Generator

def stream_batches(data, batch_size):
    batch = []

    for item in data:
        batch.append(item)

        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


data = [1, 2, 3, 4, 5, 6, 7, 8]
batch_size = 3

gen = stream_batches(data, batch_size)

print("PROGRAM 2")
print(list(gen))
print()


# 3. Contact & Token Extractor

def extract_contacts(text):
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    phone_pattern = r'(?:\+91\s?)?[6-9]\d{9}'

    emails = re.findall(email_pattern, text)
    phones = re.findall(phone_pattern, text)

    return {
        "emails": emails,
        "phones": phones
    }


text = "Contact support at ravi.kumar@techcorp.in or admin@sales.co. Direct helpline: +91 9876543210 or call 8765432109."

print("PROGRAM 3")
print(extract_contacts(text))
print()


# 4. OOP Transaction Entity Model

class Transaction:

    def __init__(self, txn_id, customer, price, qty, discount):
        self.txn_id = txn_id
        self.customer = customer
        self.price = price
        self.qty = qty
        self.discount = discount
        self.__net_total = (price * qty) * (1 - discount)

    def get_net_total(self):
        return self.__net_total

    def to_dict(self):
        return {
            "txn_id": self.txn_id,
            "customer": self.customer,
            "net_total": self.__net_total
        }


txn = Transaction("TXN_501", "Asha", 500.0, 2, 0.10)

print("PROGRAM 4")
print(txn.get_net_total())
print(txn.to_dict())
print()


# 5. Tabular Cleaning & Category Aggregation

def analyze_sales_data(raw_records):
    df = pd.DataFrame(raw_records)

    df = df.drop_duplicates()

    df["price"] = df["price"].fillna(df["price"].mean())

    df["revenue"] = df["price"] * df["quantity"]

    result = df.groupby("category").agg(
        total_revenue=("revenue", "sum"),
        avg_revenue=("revenue", "mean")
    ).reset_index()

    return result.to_dict("records")


sample_sales = [
    {
        "txn_id": "T1",
        "category": "Electronics",
        "price": 1000.0,
        "quantity": 2
    },
    {
        "txn_id": "T2",
        "category": "Furniture",
        "price": 300.0,
        "quantity": 1
    },
    {
        "txn_id": "T3",
        "category": "Electronics",
        "price": None,
        "quantity": 1
    },
    {
        "txn_id": "T1",
        "category": "Electronics",
        "price": 1000.0,
        "quantity": 2
    }
]

print("PROGRAM 5")
print(analyze_sales_data(sample_sales))
print()


# 6. Executive KPI JSON Export

def export_kpi_summary(summary_data, top_category, output_filepath):

    report = {
        "status": "SUCCESS",
        "top_category": top_category,
        "metrics": summary_data
    }

    with open(output_filepath, "w") as file:
        json.dump(report, file, indent=2)

    with open(output_filepath, "r") as file:
        return json.load(file)


summary_data = [
    {
        "category": "Electronics",
        "total_revenue": 2650.0,
        "avg_revenue": 1325.0
    },
    {
        "category": "Furniture",
        "total_revenue": 300.0,
        "avg_revenue": 300.0
    }
]

print("PROGRAM 6")
print(export_kpi_summary(
    summary_data,
    "Electronics",
    "kpi_report.json"
))
print()


# 7. Bank Account Transaction Engine

class InsufficientFundsError(Exception):
    pass


class NegativeAmountError(Exception):
    pass


class BankAccount:

    def __init__(self, account_id, initial_balance):
        self.account_id = account_id
        self.__balance = initial_balance

    def deposit(self, amount):
        if amount <= 0:
            raise NegativeAmountError("Amount must be positive")

        self.__balance += amount

    def withdraw(self, amount):
        if amount <= 0:
            raise NegativeAmountError("Amount must be positive")

        if amount > self.__balance:
            raise InsufficientFundsError("Insufficient balance")

        self.__balance -= amount

    def get_balance(self):
        return self.__balance


acc = BankAccount("ACC1001", 500.0)

acc.deposit(200.0)
acc.withdraw(300.0)

print("PROGRAM 7")
print(acc.get_balance())
print()


# 8. Streaming LRU Data Buffer

class DataBuffer:

    def __init__(self, capacity):
        self.capacity = capacity
        self.data = OrderedDict()

    def get(self, key):
        if key in self.data:
            value = self.data.pop(key)
            self.data[key] = value
            return value

        return None

    def put(self, key, value):
        if key in self.data:
            self.data.pop(key)

        self.data[key] = value

        if len(self.data) > self.capacity:
            self.data.popitem(last=False)


buf = DataBuffer(2)

buf.put("sensor1", 24.5)
buf.put("sensor2", 28.0)

buf.get("sensor1")

buf.put("sensor3", 31.2)

print("PROGRAM 8")
print(buf.get("sensor2"))
print(buf.get("sensor1"))
print(buf.get("sensor3"))
print()


# 9. Z-Score Outlier Detector

def detect_outliers_zscore(values, threshold=2.0):

    arr = np.array(values)

    mean = np.mean(arr)
    std = np.std(arr)

    z_scores = np.abs((arr - mean) / std)

    return arr[z_scores > threshold].tolist()


metrics = [
    10.0,
    12.0,
    12.0,
    13.0,
    12.0,
    11.0,
    14.0,
    100.0,
    12.0
]

outliers = detect_outliers_zscore(metrics, threshold=2.0)

print("PROGRAM 9")
print(outliers)
print()


# 10. Top-K Frequent Log Incident Analyzer

def top_k_frequent_logs(logs, k):

    count = {}

    for log in logs:
        tag = log.split(":")[0]

        if tag in count:
            count[tag] += 1
        else:
            count[tag] = 1

    sorted_tags = sorted(
        count,
        key=lambda x: (-count[x], x)
    )

    return sorted_tags[:k]


logs = [
    "ERROR: db timeout",
    "INFO: user login",
    "ERROR: auth failed",
    "WARNING: disk low",
    "ERROR: lost connection",
    "INFO: user logout"
]

print("PROGRAM 10")
print(top_k_frequent_logs(logs, 2))
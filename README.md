# 🧪 PySpark & Pytest Data Testing Framework 🧪

This project automates the **testing of data pipelines** using **PySpark & Pytest**.

---

## 📌 Features
- **✅ Schema Validation**: Validates column names, data types, and nullability.
- **✅ Data Integrity Tests**: Placeholder for future row comparison and data completeness tests.
- **✅ Performance Tests**: Placeholder for query performance validation.
- **✅ Fixtures for Test Setup**: Centralized Spark session and data loading setup.
- **✅ PySpark DataFrames**: Testing based on DataFrames simulating a data warehouse setup.

---

## 🛠️ Installation & Setup

---

### 1️⃣ **Clone the Repository**
```sh
git clone https://github.com/LumboIshinaja/data_project_tests.git
cd data_tests
```

### 2️⃣ **Create a Virtual Environment**
```sh
python -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate      # Windows
```

### 3️⃣ **Install Dependencies**
```sh
pip install -r requirements.txt
playwright install
```

---

## 🏃 Running Tests

### **✅ Run All Tests**
```sh
pytest
```

### **✅ Run Only Schema Validation Tests**
```sh
pytest -m schema
```

### **✅ Run Only Data Integrity Tests**
```sh
pytest -m integrity
```

### **✅ Run Only Performance Tests**
```sh
pytest -m performance
```

### **✅ Run Only Business Logic Tests (within Data Integrity)**
```sh
pytest -m business_logic
```

### **✅ Run Tests in Parallel (All Tests)**
```sh
pytest -n auto
```

---


## 📂 Project Structure

```
data_tests/
│── data/                       # Sample data files
│   ├── customers.csv           # Customers data CSV
│   └── sales_data.csv          # Sales data CSV
│
│── tests/                      # Pytest test suites
│   └── test_schema.py          # Schema validation tests
│   ├── test_data_integrity.py  # Data integrity tests
│   └── test_performance.py     # Performance tests
│
│── utils/
│   ├── data_loader.py          # CSV loading with PySpark DataFrames
│   ├── schema_definitions.py   # PySpark schema definitions
│   └── data_validators.py      # Data validation helpers
│
│── conftest.py                 # Shared fixtures 
│── pytest.ini                  # Pytest configuration 
│── requirements.txt            # Python dependencies
│── README.md                   # Project documentation
```

---


## 📢 Creator
- **Milos Jovanovic** - Test Engineer

---

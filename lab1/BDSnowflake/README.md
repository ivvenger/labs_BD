# Отчёт по лабораторной работе №1

**Тема:** Нормализация данных в модель «Снежинка»

## 1. Подготовка, данные и анализ

- Через **Docker** запущен **PostgreSQL**, для работы с SQL использовала **DBeaver**.
- В БД загружены 10 CSV-файлов (`mock_data(*).csv`) — всего **10 000 строк**.
- Исходная таблица (`mock_data_raw`) содержит данные о покупателях, продавцах, товарах, магазинах, поставщиках и продажах.

## 2. Что сделано

### 2.1. Созданы таблицы (DDL)
Файл `table_1.sql`:
- удаляет старые таблицы
- создаёт измерения: `dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier`, `dim_date`
- создаёт таблицу фактов `fact_sales` со связями (внешними ключами)

### 2.2. Заполнены таблицы (DML)
Файл `table_2.sql`:
- в измерения добавлены уникальные значения из `mock_data_raw`
- факты заполнены через JOIN, чтобы связать все ключи

### 2.3. Проверка (валидация)
Файл `cleck.sql`:
- количество записей в фактах = **10 000** (совпадает с исходными данными)
- суммы продаж совпадают

## 3. Схема базы данных

```mermaid
erDiagram
    dim_customer ||--o{ fact_sales : ""
    dim_seller ||--o{ fact_sales : ""
    dim_product ||--o{ fact_sales : ""
    dim_store ||--o{ fact_sales : ""
    dim_supplier ||--o{ fact_sales : ""
    dim_date ||--o{ fact_sales : ""

    dim_customer {
        int customer_key PK
        string customer_id UK
        string first_name
        string last_name
        int age
        string email
        string country
    }

    dim_seller {
        int seller_key PK
        string seller_id UK
        string first_name
        string last_name
        string email
    }

    dim_product {
        int product_key PK
        string product_id UK
        string name
        string category
        decimal price
    }

    dim_store {
        int store_key PK
        string store_name UK
        string city
        string country
    }

    dim_supplier {
        int supplier_key PK
        string supplier_name UK
        string contact
        string email
    }

    dim_date {
        int date_key PK
        date full_date UK
        int year
        int month
        string month_name
    }

    fact_sales {
        int sales_key PK
        int quantity
        decimal total_price
        int customer_key FK
        int seller_key FK
        int product_key FK
        int store_key FK
        int supplier_key FK
        int date_key FK
    }
```

## 4. Результат

Исходные данные успешно преобразованы в нормализованную модель «Снежинка».  
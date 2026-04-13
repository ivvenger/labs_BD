from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, desc

spark = SparkSession.builder \
    .appName("Reports to ClickHouse") \
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.7.1.jar,/usr/local/spark/jars/clickhouse-jdbc-0.4.6-all.jar") \
    .getOrCreate()

fact_sales = spark.read.jdbc("jdbc:postgresql://postgres:5432/bigdata_lab2", "fact_sales",
                              properties={"user": "admin", "password": "admin"})

dim_customer = spark.read.jdbc("jdbc:postgresql://postgres:5432/bigdata_lab2", "dim_customer",
                                properties={"user": "admin", "password": "admin"})
dim_seller = spark.read.jdbc("jdbc:postgresql://postgres:5432/bigdata_lab2", "dim_seller",
                              properties={"user": "admin", "password": "admin"})
dim_product = spark.read.jdbc("jdbc:postgresql://postgres:5432/bigdata_lab2", "dim_product",
                               properties={"user": "admin", "password": "admin"})
dim_store = spark.read.jdbc("jdbc:postgresql://postgres:5432/bigdata_lab2", "dim_store",
                             properties={"user": "admin", "password": "admin"})
dim_supplier = spark.read.jdbc("jdbc:postgresql://postgres:5432/bigdata_lab2", "dim_supplier",
                                properties={"user": "admin", "password": "admin"})
dim_date = spark.read.jdbc("jdbc:postgresql://postgres:5432/bigdata_lab2", "dim_date",
                            properties={"user": "admin", "password": "admin"})

def write_to_ch(df, table_name):
    df.write \
        .mode("append") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .jdbc("jdbc:clickhouse://clickhouse:8123/reports", table_name,
              properties={"user": "default", "password": ""})

report1 = fact_sales \
    .join(dim_product.select("product_key", "product_id", "name", "category"), "product_key") \
    .groupBy("product_id", "name", "category") \
    .agg(spark_sum("total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(10)
write_to_ch(report1, "report_products")
print("Отчёт report_products")

report2 = fact_sales \
    .join(dim_customer.select("customer_key", "customer_id", "first_name", "last_name", "email", "country"), "customer_key") \
    .groupBy("customer_id", "first_name", "last_name", "email", "country") \
    .agg(spark_sum("total_price").alias("total_spent"), count("sales_key").alias("purchases")) \
    .withColumn("avg_check", col("total_spent") / col("purchases")) \
    .orderBy(desc("total_spent")) \
    .limit(10)
write_to_ch(report2, "report_customers")
print("Отчёт report_customers")

report3 = fact_sales \
    .join(dim_date.select("date_key", "year", "month"), "date_key") \
    .groupBy("year", "month") \
    .agg(spark_sum("total_price").alias("monthly_revenue")) \
    .orderBy("year", "month")
write_to_ch(report3, "report_time")
print("Отчёт report_time")

report4 = fact_sales \
    .join(dim_store.select("store_key", "store_name", "city", "country"), "store_key") \
    .groupBy("store_name", "city", "country") \
    .agg(spark_sum("total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(5)
write_to_ch(report4, "report_stores")
print("Отчёт report_stores")

report5 = fact_sales \
    .join(dim_supplier.select("supplier_key", "supplier_name", "country"), "supplier_key") \
    .groupBy("supplier_name", "country") \
    .agg(spark_sum("total_price").alias("revenue")) \
    .orderBy(desc("revenue")) \
    .limit(5)
write_to_ch(report5, "report_suppliers")
print("Отчёт report_suppliers")

report6 = dim_product.select("product_id", "name", "rating", "reviews") \
    .filter(col("rating").isNotNull()) \
    .orderBy(desc("reviews")) \
    .limit(10)
write_to_ch(report6, "report_quality")
print("Отчёт report_quality")
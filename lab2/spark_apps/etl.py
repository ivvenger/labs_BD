from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, date_format
from pyspark.sql.types import IntegerType, DecimalType

spark = SparkSession.builder \
    .appName("ETL to Snowflake") \
    .config("spark.jars", "/usr/local/spark/jars/postgresql-42.7.1.jar") \
    .getOrCreate()

df_raw = spark.read.jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "mock_data_raw",
    properties={"user": "admin", "password": "admin"}
)

dim_customer = df_raw.select(
    col("sale_customer_id").cast(IntegerType()).alias("customer_id"),
    col("customer_first_name").alias("first_name"),
    col("customer_last_name").alias("last_name"),
    col("customer_age").cast(IntegerType()).alias("age"),
    col("customer_email").alias("email"),
    col("customer_country").alias("country")
).dropDuplicates(["customer_id"]).filter(col("customer_id").isNotNull())

dim_customer.write.mode("overwrite").jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_customer",
    properties={"user": "admin", "password": "admin"}
)
print("dim_customer")

dim_seller = df_raw.select(
    col("sale_seller_id").cast(IntegerType()).alias("seller_id"),
    col("seller_first_name").alias("first_name"),
    col("seller_last_name").alias("last_name"),
    col("seller_email").alias("email"),
    col("seller_country").alias("country")
).dropDuplicates(["seller_id"]).filter(col("seller_id").isNotNull())

dim_seller.write.mode("overwrite").jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_seller",
    properties={"user": "admin", "password": "admin"}
)
print("dim_seller")

dim_product = df_raw.select(
    col("sale_product_id").cast(IntegerType()).alias("product_id"),
    col("product_name").alias("name"),
    col("product_category").alias("category"),
    col("product_price").cast(DecimalType(10,2)).alias("price"),
    col("product_brand").alias("brand")
).dropDuplicates(["product_id"]).filter(col("product_id").isNotNull())

dim_product.write.mode("overwrite").jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_product",
    properties={"user": "admin", "password": "admin"}
)
print("dim_product")

dim_store = df_raw.select(
    col("store_name"),
    col("store_city").alias("city"),
    col("store_country").alias("country")
).dropDuplicates(["store_name"]).filter(col("store_name").isNotNull())

dim_store.write.mode("overwrite").jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_store",
    properties={"user": "admin", "password": "admin"}
)
print("dim_store")

dim_supplier = df_raw.select(
    col("supplier_name"),
    col("supplier_country").alias("country")
).dropDuplicates(["supplier_name"]).filter(col("supplier_name").isNotNull())

dim_supplier.write.mode("overwrite").jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_supplier",
    properties={"user": "admin", "password": "admin"}
)
print("dim_supplier")

dim_date = df_raw.select(
    to_date(col("sale_date"), "M/d/yyyy").alias("full_date")
).dropDuplicates(["full_date"]).filter(col("full_date").isNotNull()) \
    .withColumn("year", year(col("full_date"))) \
    .withColumn("month", month(col("full_date"))) \
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))

dim_date.write.mode("overwrite").jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_date",
    properties={"user": "admin", "password": "admin"}
)
print("dim_date")

product_ratings = df_raw.select(
    col("sale_product_id").cast(IntegerType()).alias("product_id"),
    col("product_rating").cast(DecimalType(3,1)).alias("rating"),
    col("product_reviews").cast(IntegerType()).alias("reviews")
).dropDuplicates(["product_id"]).filter(col("product_id").isNotNull())

product_ratings.createOrReplaceTempView("temp_ratings")
dim_product.createOrReplaceTempView("dim_product_temp")

dim_product_updated = spark.sql("""
    SELECT 
        d.product_key,
        d.product_id,
        d.name,
        d.category,
        d.price,
        d.brand,
        t.rating,
        t.reviews
    FROM dim_product_temp d
    LEFT JOIN temp_ratings t ON d.product_id = t.product_id
""")

dim_product_updated.write.mode("overwrite").jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_product",
    properties={"user": "admin", "password": "admin"}
)
print("dim_product")

dim_customer_pk = spark.read.jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_customer",
    properties={"user": "admin", "password": "admin"}
)
dim_seller_pk = spark.read.jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_seller",
    properties={"user": "admin", "password": "admin"}
)
dim_product_pk = spark.read.jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_product",
    properties={"user": "admin", "password": "admin"}
)
dim_store_pk = spark.read.jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_store",
    properties={"user": "admin", "password": "admin"}
)
dim_supplier_pk = spark.read.jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_supplier",
    properties={"user": "admin", "password": "admin"}
)
dim_date_pk = spark.read.jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "dim_date",
    properties={"user": "admin", "password": "admin"}
)

fact_sales = df_raw \
    .join(dim_customer_pk, df_raw.sale_customer_id.cast(IntegerType()) == dim_customer_pk.customer_id, "left") \
    .join(dim_seller_pk, df_raw.sale_seller_id.cast(IntegerType()) == dim_seller_pk.seller_id, "left") \
    .join(dim_product_pk, df_raw.sale_product_id.cast(IntegerType()) == dim_product_pk.product_id, "left") \
    .join(dim_store_pk, df_raw.store_name == dim_store_pk.store_name, "left") \
    .join(dim_supplier_pk, df_raw.supplier_name == dim_supplier_pk.supplier_name, "left") \
    .join(dim_date_pk, to_date(df_raw.sale_date, "M/d/yyyy") == dim_date_pk.full_date, "left") \
    .select(
        col("customer_key"),
        col("seller_key"),
        col("product_key"),
        col("store_key"),
        col("supplier_key"),
        col("date_key"),
        df_raw.sale_quantity.cast(IntegerType()).alias("quantity"),
        df_raw.sale_total_price.cast(DecimalType(10,2)).alias("total_price")
    )

fact_sales.write.mode("overwrite").jdbc(
    "jdbc:postgresql://postgres:5432/bigdata_lab2",
    "fact_sales",
    properties={"user": "admin", "password": "admin"}
)
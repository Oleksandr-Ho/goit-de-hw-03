# ***************************************
# 1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.
# ***************************************
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

# Створюємо Spark-сесію
spark = SparkSession.builder \
    .appName("GoIT Homework 03") \
    .getOrCreate()

# Шлях до файлів
users_path = "../data/users.csv"
purchases_path = "../data/purchases.csv"
products_path = "../data/products.csv"

# Завантаження CSV-файлів
users_df = spark.read.csv(users_path, header=True, inferSchema=True)
purchases_df = spark.read.csv(purchases_path, header=True, inferSchema=True)
products_df = spark.read.csv(products_path, header=True, inferSchema=True)

# Перегляд структури даних
print("Users DataFrame")
users_df.show()

print("Purchases DataFrame")
purchases_df.show()

print("Products DataFrame")
products_df.show()

# Перегляд схем даних
print("Users Schema")
users_df.printSchema()

print("Purchases Schema")
purchases_df.printSchema()

print("Products Schema")
products_df.printSchema()

# ***************************************
# 2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.
# ***************************************

# Видалені рядки (фільтруємо рядки з null-значеннями для кожної колонки)
removed_users = users_df.filter(
    col("user_id").isNull() | col("name").isNull() | col("age").isNull() | col("email").isNull()
)
removed_purchases = purchases_df.filter(
    col("purchase_id").isNull() | col("user_id").isNull() | col("product_id").isNull() |
    col("date").isNull() | col("quantity").isNull()
)
removed_products = products_df.filter(
    col("product_id").isNull() | col("product_name").isNull() |
    col("category").isNull() | col("price").isNull()
)

# Очищення даних (видалення рядків із пропущеними значеннями)
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# Виведення видалених рядків
print("Removed Users Rows")
removed_users.show()

print("Removed Purchases Rows")
removed_purchases.show()

print("Removed Products Rows")
removed_products.show()

# Виведення очищених даних
print("Cleaned Users DataFrame")
users_df.show()

print("Cleaned Purchases DataFrame")
purchases_df.show()

print("Cleaned Products DataFrame")
products_df.show()

#***************************************
# 3. Визначте загальну суму покупок за кожною категорією продуктів.
# ***************************************

# Об'єднання даних про покупки та продукти
purchases_products_df = purchases_df.join(products_df, "product_id")

# Додаємо нову колонку для вартості кожної покупки
purchases_products_df = purchases_products_df.withColumn(
    "total_value", col("quantity") * col("price")
)

# Групуємо за категорією і обчислюємо загальну суму
category_totals = purchases_products_df.groupBy("category").agg(
    sum("total_value").alias("total_sales")
)

# Виведення результатів
print("Total sales by category:")
category_totals.show()

# ***************************************
# 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.
# ***************************************

# Фільтруємо користувачів за віковою категорією
filtered_users_df = users_df.filter((col("age") >= 18) & (col("age") <= 25))

# Об'єднуємо з таблицею покупок
filtered_purchases_df = filtered_users_df.join(purchases_df, "user_id")

# Використовуємо існуючу колонку total_value
filtered_purchases_products_df = filtered_purchases_df.join(purchases_products_df, ["product_id", "user_id"])

# Групуємо за категорією і обчислюємо загальну суму
filtered_category_totals = filtered_purchases_products_df.groupBy("category").agg(
    sum("total_value").alias("total_sales")
)

# Виведення результатів
print("Total sales by category for age group 18-25:")
filtered_category_totals.show()

# ***************************************
# 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.
# ***************************************

# Обчислюємо сумарні витрати для всіх категорій
total_sales_18_25 = filtered_category_totals.agg(
    sum("total_sales").alias("grand_total")
).collect()[0]["grand_total"]

# Додаємо колонку із часткою витрат
category_percentage = filtered_category_totals.withColumn(
    "percentage", round((col("total_sales") / total_sales_18_25) * 100, 2)
)

# Виведення результатів
print("Percentage of sales by category for age group 18-25:")
category_percentage.show()

# ***************************************
# 6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.
# ***************************************

# Сортуємо за відсотком витрат і обмежуємо результат до 3 категорій
top_3_categories = category_percentage.orderBy(col("percentage").desc()).limit(3)

# Виведення результатів
print("Top 3 categories by percentage of sales for age group 18-25:")
top_3_categories.show()
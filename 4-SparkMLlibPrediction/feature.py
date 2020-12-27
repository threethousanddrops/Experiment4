from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("Feature").getOrCreate()
userinfo = spark.read.option("header", True).csv("file:///D:/a/data_format1/user_info_format1.csv")
userLog = spark.read.option("header", True).csv("file:///D:/a/data_format1/user_log_format1.csv")
train = spark.read.option("header", True).csv("file:///D:/a/data_format1/train_format1.csv")
test = spark.read.option("header", True).csv("file:///D:/a/data_format1/test_format1.csv")

userinfo = userinfo.withColumn('age_range', when(userinfo.age_range.isNull(), lit('0')).otherwise(userinfo.age_range))
userinfo = userinfo.withColumn('gender', when(userinfo.gender.isNull(), lit('2')).otherwise(userinfo.gender))

train = train.join(userinfo, on="user_id", how="left")

total_logs_temp = userLog.groupby("user_id", "seller_id").count()
##print(total_logs_temp.head())
total_logs_temp = total_logs_temp.withColumnRenamed("seller_id", "merchant_id")
total_logs_temp = total_logs_temp.withColumnRenamed("count", "total_logs")
train = train.join(total_logs_temp, on=["user_id", "merchant_id"], how="left")

unique_item_ids_temp = userLog.groupby("user_id", "seller_id", "item_id").count()
#print(unique_item_ids_temp.head())
unique_item_ids_temp = unique_item_ids_temp.drop("count")
unique_item_ids_temp1 = unique_item_ids_temp.groupby("user_id", "seller_id").count()
unique_item_ids_temp1 = unique_item_ids_temp1.withColumnRenamed("seller_id", "merchant_id")
unique_item_ids_temp1 = unique_item_ids_temp1.withColumnRenamed("count", "unique_item_ids")

train = train.join(unique_item_ids_temp1, on=["user_id", "merchant_id"], how="left")

categories_temp = userLog.groupby("user_id", "seller_id", "cat_id").count()
#print(categories_temp.head())
categories_temp = categories_temp.drop("count")
categories_temp1 = categories_temp.groupby("user_id", "seller_id").count()
categories_temp1 = categories_temp1.withColumnRenamed("seller_id", "merchant_id")
categories_temp1 = categories_temp1.withColumnRenamed("count", "categories")

train = train.join(categories_temp1, on=["user_id", "merchant_id"], how="left")

browse_days_temp = userLog.groupby("user_id", "seller_id", "time_stamp").count()
# print(browse_days_temp.head())
browse_days_temp = browse_days_temp.drop("count")
browse_days_temp1 = browse_days_temp.groupby("user_id", "seller_id").count()
browse_days_temp1 = browse_days_temp1.withColumnRenamed("seller_id", "merchant_id")
browse_days_temp1 = browse_days_temp1.withColumnRenamed("count", "browse_days")

train = train.join(browse_days_temp1, on=["user_id", "merchant_id"], how="left")

one_clicks_temp = userLog.filter("action_type == 0")
one_clicks_temp = one_clicks_temp.groupby("user_id", "seller_id").count()
one_clicks_temp = one_clicks_temp.withColumnRenamed("seller_id", "merchant_id")
one_clicks_temp = one_clicks_temp.withColumnRenamed("count", "one_clicks")
train = train.join(one_clicks_temp, on=["user_id", "merchant_id"], how="left")

shopping_carts_temp = userLog.filter("action_type == 1")
shopping_carts_temp = shopping_carts_temp.groupby("user_id", "seller_id").count()
shopping_carts_temp = shopping_carts_temp.withColumnRenamed("seller_id", "merchant_id")
shopping_carts_temp = shopping_carts_temp.withColumnRenamed("count", "shopping_carts")
train = train.join(shopping_carts_temp, on=["user_id", "merchant_id"], how="left")

purchase_times_temp = userLog.filter("action_type == 2")
purchase_times_temp = purchase_times_temp.groupby("user_id", "seller_id").count()
purchase_times_temp = purchase_times_temp.withColumnRenamed("seller_id", "merchant_id")
purchase_times_temp = purchase_times_temp.withColumnRenamed("count", "purchase_times")
train = train.join(purchase_times_temp, on=["user_id", "merchant_id"], how="left")

favourite_times_temp = userLog.filter("action_type == 3")
favourite_times_temp = favourite_times_temp.groupby("user_id", "seller_id").count()
favourite_times_temp = favourite_times_temp.withColumnRenamed("seller_id", "merchant_id")
favourite_times_temp = favourite_times_temp.withColumnRenamed("count", "favourite_times")
train = train.join(favourite_times_temp, on=["user_id", "merchant_id"], how="left")

#test.write.csv('file:///D:/train-feature.csv', header=True, mode='error')

test = test.join(userinfo, on=["user_id", "merchant_id"], how="left")
test = test.join(total_logs_temp, on=["user_id", "merchant_id"], how="left")
test = test.join(unique_item_ids_temp1, on=["user_id", "merchant_id"], how="left")
test = test.join(categories_temp1, on=["user_id", "merchant_id"], how="left")
test = test.join(browse_days_temp1, on=["user_id", "merchant_id"], how="left")
test = test.join(one_clicks_temp, on=["user_id", "merchant_id"], how="left")
test = test.join(shopping_carts_temp, on=["user_id", "merchant_id"], how="left")
test = test.join(purchase_times_temp, on=["user_id", "merchant_id"], how="left")
test = test.join(favourite_times_temp, on=["user_id", "merchant_id"], how="left")

test.write.csv('file:///D:/test-feature.csv', header=True, mode='error')

spark.stop()



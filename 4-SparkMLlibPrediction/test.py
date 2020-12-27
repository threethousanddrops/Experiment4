from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, VectorIndexer, IndexToString, StringIndexer

spark = SparkSession.builder.appName('randomForest').getOrCreate()

df_train = spark.read.options(header="True", inferSchema="True").csv("file:///D:/train-feature.csv")
df_test = spark.read.options(header="True", inferSchema="True").csv("file:///D:/test-feature.csv")

#data = VectorAssembler(inputCols=['age_range', 'gender', 'total_logs', "unique_item_ids", "categories", "browse_days", "one_clicks", "shopping_carts", "purchase_times", "favourite_times"], outputCol="features").transform(df_train)
featureData = df_train.columns[3:]
data = VectorAssembler(inputCols=featureData, outputCol="features").transform(df_train)

dataNum = data.select("label").count()
#print("nums:".format(dataNum))

featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=5).fit(data)
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

#test
testData = df_test.columns[2:]
tests = VectorAssembler(inputCols=testData, outputCol="features").transform(df_test)
featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=5).fit(tests)

#assign higher weights to the positives(cancelled == 1).
balancingRatio = float(float(data.select("label")).where("label==0").count() / float(data. count()))
calculateWeights = udf(lambda x: balancingRatio if x == 1 else (1.0-balancingRatio), FloatType())
data = data.withColumn("classWeightCol", calculateWeights('label'))

train = data
test = tests
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", weightCol="classWeightCol", numTrees=100)

pipline = Pipeline(stages=[labelIndexer, featureIndexer, rf])

model = pipline.fit(train)

prediction = model.transform(test)
rf_prob = udf(lambda x: float(x[1]), FloatType())
prediction = prediction.withColumn("prob", rf_prob("probability"))
result = prediction.select("user_id", "merchant_id", "prob")
result.toPandas().to_csv("file:///D:/result.csv", index=False)

spark.stop()





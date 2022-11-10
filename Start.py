import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("Spark Sql basic") \
        .getOrCreate()

print(spark.sparkContext.getConf().getAll())

print("#######  WORKING WITH LARGE DATASET ####")

source_path = "./glacier-mass-balance.csv"
processed_df = spark.read.csv(source_path,header=True)
print("Schema of processed dataframe")
processed_df.printSchema()

dest_path = "./glacier-mass-balance-1.json"
processed_df.write.save(dest_path, format  ="json", header = True)
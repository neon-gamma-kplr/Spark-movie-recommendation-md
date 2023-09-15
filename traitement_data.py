from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# File location and type
file_location = "/workspace/Spark-movie-recommendation-md/app/ml-latest/movies.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

df.show()

# Enregistrement du DataFrame comme une vue temporaire
df.createOrReplaceTempView("student")

sqlDF = spark.sql("SELECT * FROM student WHERE title = 'Toy Story (1995)'")
sqlDF.show()

permanent_table_name = "movies"
df.write.format("parquet")

df.write.mode('overwrite').parquet("/workspace/Spark-movie-recommendation-md/app/ml-latest/movies.parquet")

dfm_parquet = spark.read.parquet("/workspace/Spark-movie-recommendation-md/app/ml-latest/movies.parquet")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
dfm_parquet.createOrReplaceTempView("parquetFile")
test = spark.sql("SELECT * FROM parquetFile WHERE title ='Jumanji (1995)'")
test.show()

dfm_parquet.cache()

# File location and type
file_location = "/workspace/Spark-movie-recommendation-md/app/ml-latest/ratings.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dfr = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

dfr.show()

dfr.write.mode('overwrite').parquet("/workspace/Spark-movie-recommendation-md/app/ml-latest/ratings.parquet")

dfr_parquet = spark.read.parquet("/workspace/Spark-movie-recommendation-md/app/ml-latest/ratings.parquet")

# Parquet files can also be used to create a temporary view and then used in SQL statements.
dfr_parquet.createOrReplaceTempView("parquetR")
test = spark.sql("SELECT * FROM parquetR")
test.show()

dfr_parquet.cache()




fat_boy = df.join(dfr,"movieID")

fat_boy.show()

import pyspark.sql.functions as F
fat_boy = fat_boy.withColumn("userId", F.col("userId").cast("int"))
fat_boy = fat_boy.withColumn("movieId", F.col("movieId").cast("int"))
fat_boy = fat_boy.withColumn("rating", F.col("rating").cast("int"))


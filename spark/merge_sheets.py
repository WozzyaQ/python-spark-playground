from pyspark.sql import SparkSession, DataFrame, DataFrameReader

spark: SparkSession = SparkSession.builder \
    .master("local[*]") \
    .appName("read-excel") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

excel_reader: DataFrameReader = spark.read.format("com.crealytics.spark.excel") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("treatEmptyValuesAsNulls", "true") \
    .option("path", "../data/excel/multi-sheet.xlsx")

first_month_df = excel_reader.option("dataAddress", "Month-1!A1:F1000").load()
second_month_df = excel_reader.option("dataAddress", "Month-2!A1:F1000").load()

first_month_df.show(5)
second_month_df.show(5)

united_df = first_month_df.union(second_month_df)
united_df.groupby("gender").count().show()
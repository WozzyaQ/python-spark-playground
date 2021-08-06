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
    .option("path", "..data/excel/multi-sheet.xlsx")

sheet_1_df: DataFrame = excel_reader \
    .option("dataAddress", "Sheet1!A1") \
    .load()

sheet_2_df: DataFrame = excel_reader \
    .option("dataAddress", "Sheet2!A1") \
    .load()

sheet_1_df.show()
sheet_2_df.show()


from pyspark.sql import SparkSession, DataFrame

spark: SparkSession = SparkSession.builder \
    .master("local[*]") \
    .appName("aggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

effect_of_covid_df: DataFrame = spark.read.format("csv") \
    .option("header", "true") \
    .option('inferSchema', "true") \
    .option("path", "../data/csv/effects-of-covid-19-on-trade-at-21-July-2021-provisional.csv") \
    .load()

effect_of_covid_df.show(5)

pivoted_effect_years_df = effect_of_covid_df \
    .groupby("Country") \
    .pivot("Year") \
    .sum("Value")

pivoted_effect_years_df.show()

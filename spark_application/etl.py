from pyspark.sql import SparkSession

##
# VARIABLES

PATH_LANDING_ZONE_CSV_SUBCATEGORIES = '../datalake/landing/AdventureWorks_Product_Subcategories.csv'
PATH_LANDING_ZONE_CSV_PRODUCTS = '../datalake/landing/AdventureWorks_Products.csv'

PATH_PROCESSING_ZONE_SUBCATEGORIES = '../datalake/processing/SUBCATEGORIES'
PATH_PROCESSING_ZONE_PRODUCTS = '../datalake/processing/PRODUCTS'

PATH_CURATED_ZONE = '../datalake/curated'

##
# QUERY

QUERY = """ 

  SELECT
    a.ProductName,	
    b.SubcategoryName,
    a.ModelName,
    CAST(a.ProductCost as decimal(10, 2)) as ProductCost,
    CAST(a.ProductPrice as decimal(10, 2)) as ProductPrice,
    year(current_date()) as year,
    month(current_date()) as month,
    dayofmonth(current_date()) as day    
  FROM
    df1 a
  LEFT JOIN
    df2 b
  ON
    a.ProductSubcategoryKey = b.ProductSubcategoryKey

"""

##
# SCRIPT

def csv_to_parquet(spark, path_csv, path_parquet):
  df = spark.read.option('header', True).csv(path_csv)
  return df.write.mode('overwrite').format('parquet').save(path_parquet)

def create_view(spark, path_parquet, view: str):
  df = spark.read.parquet(path_parquet) 
  df.createOrReplaceTempView(view)

def write_curated(spark, path_curated):
 
  df2 = spark.sql(QUERY)
    
  (
      df2
      .orderBy('year', ascending=False)
      .orderBy('month', ascending=False)
      .orderBy('day', ascending=False)
      .write.partitionBy('year','month','day')
      .mode('overwrite')
      .format('parquet')
      .save(path_curated)
  )


if __name__ == "__main__":
  
  spark = (
    SparkSession.builder
    .master("local[*]")
    .getOrCreate()
  )

  spark.sparkContext.setLogLevel("ERROR")

  csv_to_parquet(spark, PATH_LANDING_ZONE_CSV_PRODUCTS, PATH_PROCESSING_ZONE_PRODUCTS)
  create_view(spark, PATH_PROCESSING_ZONE_PRODUCTS, 'df1')

  csv_to_parquet(spark, PATH_LANDING_ZONE_CSV_SUBCATEGORIES, PATH_PROCESSING_ZONE_SUBCATEGORIES)
  create_view(spark, PATH_PROCESSING_ZONE_SUBCATEGORIES, 'df2')
  
  write_curated(spark, PATH_CURATED_ZONE)
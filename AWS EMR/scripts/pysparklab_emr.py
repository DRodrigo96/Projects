
print("IMPORT BIBLIOTECAS")
from pyspark.sql import SparkSession
import json
print("***------ DONE")


print("***------ SPARK SESSION... ------***")   
spark = SparkSession.builder.appName("SparkLabDataHack").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
print("***------ DONE")


print("***------ SETTING VARIABLES... ------***")   
BUCKET = "workingbucket987"
print("***------ DONE")


print("***------ LECTURA DESDE S3... ------***")  
retail_df = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", value = True)
    .option("delimiter", ";")
    .csv("s3://{}/data/retail_2021.csv".format(BUCKET))
)
print("***------ DONE")


print("***------ SCHEMA ------***")    
print(json.dumps(json.loads(retail_df._jdf.schema().json()), indent=4, sort_keys=True))
print("***------ DONE")


print("***------ COLUMNAS SELECCIONADAS ------***")
retail_df.select("quant", "uprice_usd", "total_inc").show(15)
print("***------ DONE")


print("***------ SPARK SQL TRANSFORMATION... ------***")
retail_df.createOrReplaceTempView("retail_years")
avg_variables = spark.sql(
    '''
    SELECT year, area, AVG(quant) AS avg_quant, AVG(uprice_usd) AS avg_uprice_usd, AVG(total_inc) AS avg_total_inc 
    FROM retail_years GROUP BY year, area
    '''
)
print("***------ DONE")


print("***------ COLUMNAS AGRUPADAS ------***")
avg_variables.show(15)
print("***------ DONE")


print("***------ GUARDADO EN S3... ------***")
# Spark saving
(
    avg_variables
    .write
    .option("header", True)
    .mode("overwrite")
    .csv("s3://{}/output/output_emr.csv".format(BUCKET))
)
print("***------ DONE")

spark.stop()

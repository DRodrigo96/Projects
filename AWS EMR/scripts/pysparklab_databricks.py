
####################################################################

print("***------ IMPORT BIBLIOTECAS ------***")
import json
print("***------ DONE")

####################################################################

print("***------ SETTING VARIABLES ------***")
ACCESSKEY = "ACCESSKEY"
SECRETKEY = "SECRETKEY"
BUCKET = "spark-lab-987"
print("***------ DONE")

####################################################################

print("***------ SETTING KEYS ------***") 
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESSKEY)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRETKEY)
print("***------ DONE")

####################################################################

print("***------ READ FROM S3 STORAGE ------***") 
retail_df = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", value = True)
    .option("delimiter", ";")
    .csv("s3://{}/data/retail_2021.csv".format(BUCKET))
)
print("***------ DONE")

####################################################################

print("***------ SCHEMA ------***") 
print(json.dumps(json.loads(retail_df._jdf.schema().json()), indent=4, sort_keys=True))
print("***------ DONE")

####################################################################

print("***------ COLUMNAS SELECCIONADAS ------***")
retail_df.select("quant", "uprice_usd", "total_inc").show(15)
print("***------ DONE")

####################################################################

print("***------ SPARK SQL TRANSFORMATION... ------***")
retail_df.createOrReplaceTempView("retail_years")
avg_variables = spark.sql(
    '''
    SELECT year, area, AVG(quant) AS avg_quant, AVG(uprice_usd) AS avg_uprice_usd, AVG(total_inc) AS avg_total_inc 
    FROM retail_years 
    GROUP BY year, area
    '''
)
print("***------ DONE")

####################################################################

print("***------ COLUMNAS AGRUPADAS ------***")
avg_variables.show()
print("***------ DONE")

####################################################################

print("***------ GUARDADO EN S3... ------***")
(
    avg_variables
    .write
    .option("header", True)
    .mode("overwrite")
    .csv("s3://{}/output/output_databricks.csv".format(BUCKET))
)
print("***------ DONE")

####################################################################

print("***------ LECTURA DESDE S3 PARTICIONADA (DATABRICKS)... ------***")
output_databricks = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", value = True)
    .option("delimiter", ",")
    .csv("s3://{}/output/output_databricks.csv".format(BUCKET))
)
print("***------ DONE")

####################################################################

output_databricks.show()
print("***------ DONE")

####################################################################

print("***------ LECTURA DESDE S3 PARTICIONADA (EMR)... ------***")
output_emr = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", value = True)
    .option("delimiter", ",")
    .csv("s3://{}/output/output_emr.csv".format(BUCKET))
)
print("***------ DONE")

####################################################################

output_emr.show()
print("***------ DONE")
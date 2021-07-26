
####################################################################

print("***------ IMPORT BIBLIOTECAS ------***")
import json
print("***------ DONE")

####################################################################

print("***------ SETTING VARIABLES ------***") 
CONTAINER = "dhmain987"
STORAGEACCOUNT = "testing987dh"
ACCESSKEY = "ACCESSKEY"
print("***------ DONE")

####################################################################

print("***------ SETTING KEYS ------***") 
spark.conf.set("fs.azure.account.key.{}.dfs.core.windows.net".format(STORAGEACCOUNT), ACCESSKEY)
print("***------ DONE")

####################################################################

print("***------ READ FROM DL STORAGE ------***") 
retail_df = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", value = True)
    .option("delimiter", ";")
    .csv("abfs://{}@{}.dfs.core.windows.net/data/retail_2021.csv".format(CONTAINER, STORAGEACCOUNT))
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

print("***------ GUARDADO EN DATA LAKE STORAGE GEN2... ------***")
(
    avg_variables
    .write
    .option("header", True)
    .mode("overwrite")
    .csv("abfs://{}@{}.dfs.core.windows.net/output/output_databricks.csv".format(CONTAINER, STORAGEACCOUNT))
)
print("***------ DONE")

####################################################################

print("***------ LECTURA DESDE DATA LAKE STORAGE GEN2 PARTICIONADA (DATABRICKS)... ------***")
output_databricks = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", value = True)
    .option("delimiter", ",")
    .csv("abfs://{}@{}.dfs.core.windows.net/output/output_databricks.csv".format(CONTAINER, STORAGEACCOUNT))
)
print("***------ DONE")

####################################################################

output_databricks.show()
print("***------ DONE")

####################################################################

print("***------ LECTURA DESDE DATA LAKE STORAGE GEN2 PARTICIONADA (HDINSIGHT)... ------***")
output_hdinsight = (
    spark
    .read
    .option("header", "true")
    .option("inferSchema", value = True)
    .option("delimiter", ",")
    .csv("abfs://{}@{}.dfs.core.windows.net/output/output_hdinsight.csv".format(CONTAINER, STORAGEACCOUNT))
)
print("***------ DONE")

####################################################################

output_hdinsight.show()
print("***------ DONE")
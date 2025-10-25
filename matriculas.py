#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import count

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('TareaMatriculas').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/TareaMatriculas/4wrd-zngb.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

#imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Estadisticas básicas
df.summary().show()

# CONTEO DE LA CANTIDAD DE ALUMNOS DE GENERO (MUJER-HOMBRE)
#df.groupBy("GENERO").agg(sum("ESTADO").alias("GENEROS MATRICULADOS")).show()
df.groupBy("GENERO") \
  .agg(F.sum(F.when(F.col("ESTADO") == "MATRICULADO", 1).otherwise(0))
       .alias("GENEROS_MATRICULADOS")) \
  .show()

#contar estudiantes por zona y genero
#df.groupBy("ZONA_SEDE").agg(sum("*").alias("TOTAL_ESTUDIANTES")).show()
df.groupBy("ZONA_SEDE") \
  .agg(F.count("*").alias("TOTAL_ESTUDIANTES")) \
  .orderBy(F.desc("TOTAL_ESTUDIANTES")) \
  .show()

#contar estudiantes por estracto
df.groupBy( "ESTRATO").agg(count("*").alias("TOTAL_ESTUDIANTES")).orderBy("ESTRATO").show()

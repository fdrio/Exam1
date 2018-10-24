import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
reload(sys)
sys.setdefaultencoding('utf8')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df_escuelas = spark.read.csv("hdfs://localhost/data/escuelasPR.csv")

#df_escuelas = df_escuelas.toDF("Region","Distrito_Escolar","Ciudad","Numero_de_Identificacion_de_la_Escuela","Nombre_de_Escuela","Nivel_de_la_Escuela","Numero_de_Serie_segun_College_Board_Puerto_Rico")
df_count = df_escuelas.filter(df_escuelas._c0=="Arecibo").groupBy("_c1","_c2").agg(count("*"))

df_escuelas.show()
df_count=df_count.toDF("Distrito","Ciudad","Count")
df_count.show()



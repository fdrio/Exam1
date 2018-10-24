import sys
from pyspark.sql import SparkSession
reload(sys)
sys.setdefaultencoding('utf8')

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df_escuelas = spark.read.csv("hdfs://localhost/data/escuelasPR.csv")
df_students = spark.read.csv("hdfs://localhost/data/studentsPR.csv")
#df.withColumnRenamed('_c0',"Region Educativa").withColumnRenamed('_c1',"Distrito Escolar").withColumnRenamed('_c2',"Numero de Identificacion de Escuela").withColumnRenamed('_c3',"Nombre de escuela").withColumnRenamed('_c4',"Nivel de escuela").withColumnRenamed('_c5',"Sexo").withColumnRenamed('_c6',"Id estudiante")

#print(df.columns)
#df_escuelas = df_escuelas.toDF("Region","Distrito_Escolar","Ciudad","Numero_de_Identificacion_de_la_Escuela","Nombre_de_Escuela","Nivel_de_la_Escuela","Numero_de_Serie_segun_College_Board_Puerto_Rico")


df_students = df_students.toDF("_c7","_c8","_c3","_c10","_c11","_c12","_c13")
df_escuelas.show()

df_students.show()


df_tmp = df_escuelas.join(df_students,"_c3")
df_tmp=df_tmp.filter(df_tmp._c12=="M").filter((df_tmp._c0 =="Ponce") | (df_tmp._c0=="San Juan")).filter(df_tmp._c5=="Superior").select("_c13","_c0","_c4","_c5","_c12")
df_tmp=df_tmp.toDF("Student ID","Region","Nombre de la Escuela","Level","Sexo")
df_tmp.show()
print("Count: ",df_tmp.count())
#df_count = df_tmp.groupBy(df_tmp._c13).agg({"_c13":"count"})
#df_count.show()

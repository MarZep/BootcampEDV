from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import regexp_replace, col

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

##leo csv de HDFS y lo cargo en un dataframe
df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/nifi/titanic.csv")

##creamos una vista del DF
df.createOrReplaceTempView("titanic")

##iltramos el DF para remover las columnas SibSp y Parch, calcular el promedio de edad y si el valor de cabina en nulo, dejarlo en 0 (cero)
df_titanic =  spark.sql("select cast(passengerId as int), cast(survived as int), cast(pclass as tinyint), cast(name as string), cast(sex as string), cast(age as float), cast(ticket as string), cast(fare as float), cast(cabin as string), cast(embarked as string) from titanic")

df_titanic.createOrReplaceTempView("columnas_eliminadas")

df_titanic = spark.sql("select passengerId, survived, pclass, name, sex, age, ticket, fare, cabin, embarked, AVG(age) OVER (partition by sex) as avg_age_by_sex from columnas_eliminadas")

df_titanic.createOrReplaceTempView("columnas_eliminadas_promedio")

df_titanic = spark.sql("select passengerId, survived, pclass, name, sex, age, ticket, fare, COALESCE(cabin, 0) as cabin, embarked, ROUND(avg_age_by_sex, 2) as avg_age_by_sex from columnas_eliminadas_promedio")

df_titanic = df_titanic.withColumn('name', regexp_replace(col('name'), ',', ''))

df_titanic.createOrReplaceTempView("vista_final_Load")

##insertamos el DF en la tabla titanic.information
spark.sql("insert into titanic.information select * from vista_final_Load")
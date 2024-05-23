from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

##leo csv de HDFS y lo cargo en un dataframe
df_drivers = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/drivers.csv")
df_results = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/results.csv")
df_constructors = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/constructors.csv")
df_races = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/races.csv")

##creamos una vista de los DF
df_drivers.createOrReplaceTempView("drivers")
df_results.createOrReplaceTempView("results")
df_constructors.createOrReplaceTempView("constructors")
df_races.createOrReplaceTempView("races")

##unimos y filtramos el DF para corredores con mayor cantidad de puntos en la historia
df_drivers2 =  spark.sql("select cast(driverId as int) as id, cast(forename as string) as driver_forename, cast(surname as string) as driver_surname, cast(nationality as string) as driver_nationality from drivers")
df_results2 =  spark.sql("select cast(driverId as int) as id, cast(points as float) from results")

df_drivers2.createOrReplaceTempView("drivers_filtrados")
df_results2.createOrReplaceTempView("results_filtrados")

df_final = spark.sql("select drivers_filtrados.driver_forename, drivers_filtrados.driver_surname, drivers_filtrados.driver_nationality, SUM(results_filtrados.points) as points from drivers_filtrados inner join results_filtrados on drivers_filtrados.id = results_filtrados.id where points > 0 group by drivers_filtrados.driver_forename, drivers_filtrados.driver_surname, drivers_filtrados.driver_nationality order by points desc limit 10")

df_final.createOrReplaceTempView("corredores_puntos")

##insertamos el DF en la tabla f1.driver_results
spark.sql("insert into f1.driver_results select * from corredores_puntos")

##unimos y filtramos el DF para quienes obtuvieron más puntos en el Spanish Grand Prix en el año 1991
df_constructors2 =  spark.sql("select cast(constructorId as string), cast(constructorRef as string), cast(name as string) as cons_name, cast(nationality as string) as cons_nationality, cast(url as string) from constructors")
df_results2 =  spark.sql("select cast(constructorId as int), cast(raceId as int), cast(points as float) from results")
df_races2 =  spark.sql("select cast(raceId as int), cast(year as int),cast(name as string) from races")

df_constructors2.createOrReplaceTempView("constructors_filtrados")
df_results2.createOrReplaceTempView("results_filtrados")
df_races2.createOrReplaceTempView("races_filtrados")

df_results_races = spark.sql("select * from results_filtrados inner join races_filtrados on results_filtrados.raceId = races_filtrados.raceId inner join constructors_filtrados on constructors_filtrados.constructorId = results_filtrados.constructorId")

df_results_races.createOrReplaceTempView("join_tablas")

df_final =  spark.sql("select constructorRef, cons_name, cons_nationality, url, SUM(points) as points from join_tablas where year = 1991 and name = 'Spanish Grand Prix' and points > 0 group by constructorRef, cons_name, cons_nationality, url")

df_final.createOrReplaceTempView("constructores_puntos")

##insertamos el DF en la tabla f1.constructor_results
spark.sql("insert into f1.constructor_results select * from constructores_puntos")
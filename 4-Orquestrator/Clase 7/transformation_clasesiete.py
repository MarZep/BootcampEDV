from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

##leo los parquets de HDFS y lo cargo en un dataframe
df = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.parquet")
df2 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-02.parquet")

##creamos una vista de los DF
df.createOrReplaceTempView("tripdata_vista01")
df2.createOrReplaceTempView("tripdata_vista02")

##unimos y filtramos el DF
df_filtro = spark.sql("select * from tripdata_vista01 union all select * from tripdata_vista02")

df_filtro.createOrReplaceTempView("tripdata_vista_union")

df_filtro = spark.sql("select * from tripdata_vista_union where month(tpep_pickup_datetime) <= 02 and year(tpep_pickup_datetime) = 2021")

df_filtro.createOrReplaceTempView("tripdata_vista_filtrado_fecha")

df_filtro = spark.sql("select * from tripdata_vista_filtrado_fecha where RatecodeId = 2 or coalesce(airport_fee, 0) >0 ")

df_filtro.createOrReplaceTempView("tripdata_vista_filtrado_aeropuerto")

df_filtro = spark.sql("select * from tripdata_vista_filtrado_aeropuerto where payment_type = 2")

df_filtro.createOrReplaceTempView("tripdata_vista_filtrado_payment")

df_filtro = spark.sql("select cast(tpep_pickup_datetime as date), cast(airport_fee as float), cast(payment_type as int), cast(tolls_amount as double), cast(total_amount as double) from tripdata_vista_filtrado_payment")

##Creamos una vista con la data filtrada final###
df_filtro.createOrReplaceTempView("tripdata_vista_final")

##insertamos el DF filtrado en la tabla tripdata.airport.trips
spark.sql("insert into tripdata.airports_trips select * from tripdata_vista_final")
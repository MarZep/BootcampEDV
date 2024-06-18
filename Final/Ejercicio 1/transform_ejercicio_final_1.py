from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date  
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

##leo csv desde HDFS y lo cargo en un dataframe
df = spark.read.option("header", "true").option("sep", ";",).csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
df2 = spark.read.option("header", "true").option("sep", ";",).csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")
df3 = spark.read.option("header", "true").option("sep", ";",).csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")

##elimino espacios y caracteres en los headers
df = df.select([F.col(x).alias(x.replace(' ', '').replace('ó', 'o').replace('/', '').replace('(', '').replace(')', '')) for x in df.columns])
df = df.withColumn('fecha', to_date(col('Fecha'), 'd/M/yyyy'))
df2 = df2.select([F.col(x).alias(x.replace(' ', '').replace('ó', 'o').replace('/', '').replace('(', '').replace(')', '')) for x in df2.columns])
df2 = df2.withColumn('fecha', to_date(col('Fecha'), 'd/M/yyyy'))

##creamos vistas de los df
df.createOrReplaceTempView("2021_informe")
df2.createOrReplaceTempView("202206_informe")
df3.createOrReplaceTempView("aeropuertos")

##filtramos los df
df_2021=  spark.sql("select fecha, HoraUTC as horautc, ClasedeVuelotodoslosvuelos as clase_de_vuelo, ClasificacionVuelo as clasificacion_de_vuelo, TipodeMovimiento as tipo_de_movimiento, Aeropuerto as aeropuerto, OrigenDestino as origen_destino, AerolineaNombre as aerolinea_nombre, Aeronave as aeronave, COALESCE(CAST(Pasajeros AS int),0) as pasajeros from 2021_informe where ClasificacionVuelo = 'Domestico'")

df_202206=  spark.sql("select fecha, HoraUTC as horautc, ClasedeVuelotodoslosvuelos as clase_de_vuelo, ClasificacionVuelo as clasificacion_de_vuelo, TipodeMovimiento as tipo_de_movimiento, Aeropuerto as aeropuerto, OrigenDestino as origen_destino, AerolineaNombre as aerolinea_nombre, Aeronave as aeronave, COALESCE(CAST(Pasajeros AS int),0) as pasajeros from 202206_informe where ClasificacionVuelo = 'Doméstico'")

df_2021.createOrReplaceTempView("2021_informe_cast")
df_202206.createOrReplaceTempView("202206_informe_cast")

df_union =  spark.sql("select * from 2021_informe_cast union select * from 202206_informe_cast")

df_union.createOrReplaceTempView("informes_union")

df_final = spark.sql("select * from informes_union where fecha between '2021-01-01' and '2022-06-30'")

df_final.createOrReplaceTempView("informes_tabla_final")

df_aeropuertos =  spark.sql("select local as aeropuerto, oaci as oac, iata, tipo, denominacion, coordenadas, latitud, longitud, CAST(elev AS float), uom_elev, ref, COALESCE(CAST(distancia_ref AS float),0) as distancia_ref, direccion_ref, condicion, control, region, uso, trafico,sna, concesionado, provincia from aeropuertos")

df_aeropuertos.createOrReplaceTempView("aeropuertos_tabla_final")

##insertamos los df en Hive
spark.sql("insert into aviacion.aeropuerto_tabla select * from informes_tabla_final")

spark.sql("insert into aviacion.aeropuerto_detalle_tabla select * from aeropuertos_tabla_final")
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import from_unixtime, to_date, col
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

##leo parquet de HDFS y lo cargo en un dataframe
df_envios = spark.read.parquet("hdfs://172.17.0.2:9000/sqoop/ingest/envios/*.parquet")
df_orders = spark.read.parquet("hdfs://172.17.0.2:9000/sqoop/ingest/orders_details/*.parquet")

#pasamos el campo shipped_date a tipo date
df_envios= df_envios.withColumn("shipped_date", to_date(from_unixtime(col("shipped_date")/1000)))

##creamos una vista del DF
df_envios.createOrReplaceTempView("envios")
df_orders.createOrReplaceTempView("orders")

##filtramos los df para solo de aquellos pedidos que hayan tenido descuento
df_envios = spark.sql("select order_id as order_id_env, shipped_date, company_name, phone from envios")
df_envios.createOrReplaceTempView("envios_cambio_campo")

df_join = spark.sql("select * from envios_cambio_campo e left join orders o on e.order_id_env = o.order_id where o.discount != 0")

df_join.createOrReplaceTempView("df_joined")

df_descuentos = spark.sql("select order_id, shipped_date, company_name, phone, (unit_price-(unit_price*discount)/100) as unit_price_discount, quantity, round((unit_price-(unit_price*discount)/100)*quantity,2) as total_price from df_joined")

df_descuentos.createOrReplaceTempView("descuentos_filtrados")

##insertamos el DF en la tabla northwind_analytics.products_sent
spark.sql("insert into northwind_analytics.products_sent select * from descuentos_filtrados")
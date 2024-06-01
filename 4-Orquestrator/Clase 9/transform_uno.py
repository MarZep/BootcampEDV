from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

##leo parquet de HDFS y lo cargo en un dataframe
df = spark.read.parquet("hdfs://172.17.0.2:9000/sqoop/ingest/clientes/*.parquet")

##creamos una vista del DF
df.createOrReplaceTempView("clientes_productos")

##filtramos el DF para pero obtener aquellas compañías en las que la cantidad de productos vendidos fue mayor al promedio.
df_products =  spark.sql("select customer_id, company_name, cast(productos_vendidos as int) from clientes_productos where productos_vendidos > (select AVG(cast(productos_vendidos as int)) from clientes_productos) order by productos_vendidos desc")

df_products.createOrReplaceTempView("products_filtrados")

##insertamos el DF en la tabla northwind_analytics.products_sold
spark.sql("insert into northwind_analytics.products_sold select * from products_filtrados")
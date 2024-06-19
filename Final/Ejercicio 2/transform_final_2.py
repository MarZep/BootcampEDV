from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

##leo csv desde HDFS y lo cargo en un dataframe
df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")
df2 = spark.read.option("header", "true").option("sep", ";",).csv("hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv")

##creamos vistas de los df
df.createOrReplaceTempView("CarRentalData")
df2.createOrReplaceTempView("georef")

##filtramos los df
df_RentalData=  spark.sql("select cd.fuelType, CAST(ROUND(CAST(cd.rating as float),0) as int) as rating, CAST(cd.renterTripsTaken as int), CAST(cd.reviewCount as integer), cd.`location.city` as city, gf.`Official Name State` as state_name, CAST(cd.`owner.id` as int) as owner_id, CAST(cd.`rate.daily` as int) as rate_daily, cd.`vehicle.make`as make, cd.`vehicle.model` as model, CAST(cd.`vehicle.year` as int) as year  from CarRentalData cd left join georef  gf on cd.`location.state` = gf.`United States Postal Service state abbreviation`")

df_RentalData.createOrReplaceTempView("cast_joined")

df_RentalData =  spark.sql("select LOWER(fuelType), rating, renterTripsTaken, reviewCount, city, state_name, owner_id, rate_daily, make, model, year from cast_joined where rating IS NOT NULL and state_name != 'Texas'")

df_RentalData.createOrReplaceTempView("final_table")

##insertamos en Hive
spark.sql("insert into car_rental_db.car_rental_analytics select * from final_table")
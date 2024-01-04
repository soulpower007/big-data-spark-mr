# get the spark context
from pyspark import SparkContext, SparkConf
import sys

#cf = SparkConf()
#cf.set("spark.submit.deployMode","client")
#sc = SparkContext.getOrCreate(cf)

from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()



df_trip = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[1])

df_fare = spark.read.format('csv').options(header='true',
inferschema='true').load(sys.argv[2])

df_fare.createOrReplaceTempView("fare")

df_trip.createOrReplaceTempView("trip")

#trip x fare
join = spark.sql("""select 
                 trip.medallion, trip.hack_license, trip.vendor_id, trip.pickup_datetime, trip.rate_code,trip.store_and_fwd_flag,trip.dropoff_datetime,trip.passenger_count,trip.trip_time_in_secs,trip.trip_distance,trip.pickup_longitude,trip.pickup_latitude,trip.dropoff_longitude,trip.dropoff_latitude,
                 fare.payment_type, fare.fare_amount, fare.surcharge, fare.mta_tax, fare.tip_amount, fare.tolls_amount, fare.total_amount
                 from trip as trip  inner join fare as fare on 
                 fare.medallion = trip.medallion and 
                 fare.hack_license  = trip.hack_license and
                 fare.vendor_id = trip.vendor_id and 
                 fare.pickup_datetime = trip.pickup_datetime
                 order by trip.medallion, trip.hack_license, trip.pickup_datetime""")
 

join.show()
join.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',join.medallion, join.hack_license, join.vendor_id, join.pickup_datetime, join.rate_code,join.store_and_fwd_flag,join.dropoff_datetime,join.passenger_count,join.trip_time_in_secs,join.trip_distance,join.pickup_longitude,join.pickup_latitude,join.dropoff_longitude,join.dropoff_latitude,
                 join.payment_type, join.fare_amount, join.surcharge, join.mta_tax, join.tip_amount, join.tolls_amount, join.total_amount)).write.save('task1a-sql.out', format='text')


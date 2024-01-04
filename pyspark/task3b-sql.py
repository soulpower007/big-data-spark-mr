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

df_trips = spark.read.format('csv').options(header='false',
inferschema='false').load(sys.argv[1])

df_trips.createOrReplaceTempView("trips")
#df_trips.show()

#trip x fare
join = spark.sql(""" 
                 select x.medallion as medallion, x.pickup_datetime as pickup_datetime from (
                select trips._c0 as medallion, trips._c3 as pickup_datetime, count(*) as multiple_entry from trips
                group by trips._c0, trips._c3) as x
                where x.multiple_entry > 1
                order by medallion , pickup_datetime
""")

# medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount15,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,
 

#df_trips.show()
join.createOrReplaceTempView("AllTrips")
#print(df_trips.columns)
# join.show()
join.select(format_string('%s,%s',join.medallion,  join.pickup_datetime)).write.save('task3b-sql.out', format='text')

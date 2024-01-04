from pyspark import SparkContext, SparkConf
import sys

# cf = SparkConf()
# cf.set("spark.submit.deployMode","client")
# sc = SparkContext.getOrCreate(cf)

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

#trip x fare
join = spark.sql(""" select 0 as one, 5 as two, count(*) as num_trips from trips where trips._c15 >= 0 and trips._c15 <= 5 union
                     select 5 as one, 15 as two, count(*) as num_trips from trips where trips._c15 > 5 and trips._c15 <=15 union
                 select 15 as one, 30 as two, count(*) as num_trips from trips where trips._c15 > 15 and trips._c15 <=30 union
                 select 30 as one, 50 as two, count(*) as num_trips from trips where trips._c15 > 30 and trips._c15 <=50 union
                 select 50 as one, 100 as two, count(*) as num_trips from trips where trips._c15 > 50 and trips._c15 <=100 union
                 select 100 as one, '>' as two, count(*) as num_trips from trips where trips._c15 > 100
                 order by one""")

# medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,
 

#df_trips.show()
join.createOrReplaceTempView("AllTrips")
#print(df_trips.columns)
join.select(format_string('%s,%s,%s',join.one, join.two, join.num_trips)).write.save('task2a.out', format='text')

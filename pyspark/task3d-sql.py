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

#trip x fare
join = spark.sql(""" 
                select x.hack_license as hack_license, count(x.medallion) as num_taxis_used 
                 from 
                (select trips._c1 as hack_license , trips._c0 as medallion
                from trips
                group by trips._c1, trips._c0
                ) as x 
                 group by x.hack_license
                 order by x.hack_license
                 
""")

# medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount15,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,
 

#df_trips.show()
join.createOrReplaceTempView("AllTrips")
#print(df_trips.columns)
#join.show()
join.select(format_string('%s,%s',join.hack_license,  join.num_taxis_used)).write.save('task3d-sql.out', format='text')

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
inferschema='true').load(sys.argv[1])

df_trips.createOrReplaceTempView("trips2")

#trip x fare
#spark.sql(""" select trips2._c16, trips2._c5, trips2._c8, trips2._c6 from trips2""").show()
join = spark.sql("""
                  select trips2._c16 as vehicle_type, count(*) as total_trips, 
                  sum(trips2._c5) as  total_revenue, 
                  sum(trips2._c8)*100/ sum(trips2._c5)  as avg_tip_percentage
                  from trips2
                  group by trips2._c16
                  order by trips2._c16""")

# """select agent_name, from()"""

# medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,
 

#df_trips.show()
#join.createOrReplaceTempView("AllTrips")
#print(df_trips.columns)
join.show()
join.select(format_string('%s,%s,%s,%s',join.vehicle_type,  join.total_trips, join.total_revenue, join.avg_tip_percentage)).write.save('task4a-sql.out', format='text')


            #    _c0|                 _c1|_c2|                _c3|_c4|          _c5|         _c6|         _c7|          _c8|         _c9|         _c10|              _c11|     _c12|_c13| _c14|             _c15|_c16|_c17|            _c18|_c19|                _c20|         _c21|_c22|                _c23|     _c24| _c25|

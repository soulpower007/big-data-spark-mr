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
                select x.medallion as medallion, ( (case when y.no_cords_count is null then 0 else y.no_cords_count end) / x.all_count)*100 as percentage 
                from 
                (select trips._c0 as medallion , count(*) as all_count
                from trips
                group by trips._c0) as x
                left join 
                (select trips._c0 as medallion, count(*) as no_cords_count
                from trips
                where trips._c10 = 0 and trips._c11 = 0
                group by trips._c0 ) as y
                on x.medallion = y.medallion
                order by x.medallion
""")

# medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount15,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,
 

#df_trips.show()
join.createOrReplaceTempView("AllTrips")
#print(df_trips.columns)
#join.show()
join.select(format_string('%s,%.2f',join.medallion,  join.percentage)).write.save('task3c-sql.out', format='text')


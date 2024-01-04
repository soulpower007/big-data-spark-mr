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
x= """SELECT grp1.medallion, count(*) as days_driven, avg(grp1.trips_on_day) as average
FROM (
    SELECT trips._c0 as medallion, date_format(cast(trips._c3 as date),'yyyy-MM-dd') as day, count(*) as trips_on_day
    FROM trips
    GROUP BY medallion, date_format(cast(trips._c3 as date),'yyyy-MM-dd')
) as grp1
GROUP BY medallion;
"""
y=""" select trips._c0 as medallion, count(*) as total_trips from trips group by trips._c0 """

join = spark.sql("""SELECT x.medallion as medallion, y.total_trips as total_trips, x.days_driven as days_driven, x.average as average
FROM (
    SELECT grp1.medallion as medallion, count(*) as days_driven, avg(grp1.trips_on_day) as average
    FROM (
        SELECT trips._c0 as medallion, date_format(cast(trips._c3 as date),'yyyy-MM-dd') as day, count(*) as trips_on_day
        FROM trips
        GROUP BY trips._c0, date_format(cast(trips._c3 as date),'yyyy-MM-dd')
    ) as grp1
    GROUP BY grp1.medallion
) as x
INNER JOIN (
    SELECT trips._c0 as medallion, count(*) as total_trips
    FROM trips
    GROUP BY trips._c0
) as y
ON x.medallion = y.medallion
order by medallion
 """)

# """ select 
#                  from trips
#                  group by taxi"""
# avg trips per day


# medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,
 


#df_trips.show()
join.createOrReplaceTempView("AllTrips")
#print(df_trips.columns)
# join.show() 
join.select(format_string('%s,%s,%s,%.2f',join.medallion, join.total_trips, join.days_driven, join.average)).write.save('task2d-sql.out', format='text')

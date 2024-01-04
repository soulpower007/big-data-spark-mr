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

#spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
df_trips = spark.read.format('csv').options(header='false',
inferschema='false').load(sys.argv[1])

df_trips.createOrReplaceTempView("trips")



join = spark.sql(""" select date_format(cast(trips._c3 as date), 'yyyy-MM-dd') AS date, sum(trips._c15)+sum(trips._c16)+sum(trips._c18) as total_revenue, sum(trips._c19) as total_tolls
    from trips    group by date_format(cast(trips._c3 as date), 'yyyy-MM-dd')
                 order by date_format(cast(trips._c3 as date), 'yyyy-MM-dd') """)

# medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount15,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,
 

#df_trips.show()
join.createOrReplaceTempView("AllTrips")
#print(df_trips.columns)
#join.show()
join.select(format_string('%s,%.2f,%.2f',join.date, join.total_revenue, join.total_tolls)).write.save('task2c-sql.out', format='text')

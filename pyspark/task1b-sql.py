# get the spark context
from pyspark import SparkContext,SparkConf
from pyspark.sql.functions import format_string
import sys

cf = SparkConf()
cf.set("spark.submit.deployMode","client")
sc = SparkContext.getOrCreate(cf)

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option","some-value") \
    .getOrCreate()



df_fare = spark.read.format('csv').options(header='true',
inferschema='true',ignoreTrailingWhiteSpace="true").load(sys.argv[1])
# '/shared/CS-GY-6513/hw-map-reduce/Data/fare_data_shorter.csv'
df_license = spark.read.format('csv').options(header='true',
inferschema='true', ignoreTrailingWhiteSpace="true").load(sys.argv[2])

df_fare.createOrReplaceTempView("fare")

df_license.createOrReplaceTempView("license")

# fare x license

# license.name,license.type,license.curr    ent_status,license.DMV_license_plate,license.vehicle_VIN_number,license.vehicle_type,license.model_year,license.medallion_type,license.agent_number,license.agent_name,license.agent_telephone_number,license.agent_website,license.agent_address,license.last_updated_date,license.last_updated_time


join = spark.sql("""select 
                 fare.medallion,fare.hack_license,fare.vendor_id,fare.pickup_datetime,fare.payment_type,fare.fare_amount,fare.surcharge,fare.mta_tax,fare.tip_amount,fare.tolls_amount,fare.total_amount,
                 license.name,license.type,license.current_status,license.DMV_license_plate,license.vehicle_VIN_number,license.vehicle_type,license.model_year,license.medallion_type,license.agent_number,license.agent_name,license.agent_telephone_number,license.agent_website,license.agent_address,license.last_updated_date,license.last_updated_time
                 from  fare inner join license on 
                 fare.medallion = license.medallion
                 order by fare.medallion,fare.hack_license,fare.pickup_datetime""")
# join.createOrReplaceTempView("1b")
# join.show()
join.write.option("quoteAll","true").option("ignoreTrailingWhiteSpace","true").save('task1b-sql.out',format='csv')


#join.show()
#join.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',
#                 join.medallion,join.hack_license,join.vendor_id,join.pickup_datetime,join.payment_type,join.fare_amount,join.surcharge,join.mta_tax,join.tip_amount,join.tolls_amount,join.total_amount,
#                 join.name,join.type,join.current_status,join.DMV_license_plate,join.vehicle_VIN_number,join.vehicle_type,join.model_year,join.medallion_type,join.agent_number,join.agent_name,join.agent_telephone_number,join.agent_website,join.agent_address,join.last_updated_date,join.last_updated_time)).write.option("ignoreTrailingWhiteSpace","true").option("quoteAll", "true").option("quote","\"").save('task1b-sql.out',format='text')




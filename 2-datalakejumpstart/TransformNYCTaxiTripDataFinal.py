import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
import re

# Script generated for node Remove Records with NULL
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF().na.drop()
    results = DynamicFrame.fromDF(df, glueContext, "results")
    return DynamicFrameCollection({"results": results}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Dropoff Zone Lookup
DropoffZoneLookup_node1728658721911 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_taxi_zone_lookup", transformation_ctx="DropoffZoneLookup_node1728658721911")

# Script generated for node Yellow Trip Data
YellowTripData_node1728651976903 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_yellow_tripdata", transformation_ctx="YellowTripData_node1728651976903")

# Script generated for node Pickup Zone Lookup
PickupZoneLookup_node1728654497979 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_taxi_zone_lookup", transformation_ctx="PickupZoneLookup_node1728654497979")

# Script generated for node Change Schema - Dropoff Zone Lookup
ChangeSchemaDropoffZoneLookup_node1728658855848 = ApplyMapping.apply(frame=DropoffZoneLookup_node1728658721911, mappings=[("locationid", "long", "do_locationid", "long"), ("borough", "string", "do_borough", "string"), ("zone", "string", "do_zone", "string"), ("service_zone", "string", "do_service_zone", "string")], transformation_ctx="ChangeSchemaDropoffZoneLookup_node1728658855848")

# Script generated for node Remove Records with NULL
RemoveRecordswithNULL_node1728653405295 = MyTransform(glueContext, DynamicFrameCollection({"YellowTripData_node1728651976903": YellowTripData_node1728651976903}, glueContext))

# Script generated for node Change Schema - Pickup Zone Lookup
ChangeSchemaPickupZoneLookup_node1728654802898 = ApplyMapping.apply(frame=PickupZoneLookup_node1728654497979, mappings=[("locationid", "long", "pu_locationid", "long"), ("borough", "string", "pu_borough", "string"), ("zone", "string", "pu_zone", "string"), ("service_zone", "string", "pu_service_zone", "string")], transformation_ctx="ChangeSchemaPickupZoneLookup_node1728654802898")

# Script generated for node SelectFromCollection
SelectFromCollection_node1728653661921 = SelectFromCollection.apply(dfc=RemoveRecordswithNULL_node1728653405295, key=list(RemoveRecordswithNULL_node1728653405295.keys())[0], transformation_ctx="SelectFromCollection_node1728653661921")

# Script generated for node Filter - Yellow Trip Data
FilterYellowTripData_node1728653780026 = Filter.apply(frame=SelectFromCollection_node1728653661921, f=lambda row: (bool(re.match("^2020-1", row["tpep_pickup_datetime"]))), transformation_ctx="FilterYellowTripData_node1728653780026")

# Script generated for node Yellow Trips Data + Pickup Zone Lookup
YellowTripsDataPickupZoneLookup_node1728655079416 = Join.apply(frame1=ChangeSchemaPickupZoneLookup_node1728654802898, frame2=FilterYellowTripData_node1728653780026, keys1=["pu_locationid"], keys2=["pulocationid"], transformation_ctx="YellowTripsDataPickupZoneLookup_node1728655079416")

# Script generated for node Yellow Trips Data + Pickup Zone Lookup + Dropoff Zone Lookup
YellowTripsDataPickupZoneLookupDropoffZoneLookup_node1728658981088 = Join.apply(frame1=ChangeSchemaDropoffZoneLookup_node1728658855848, frame2=YellowTripsDataPickupZoneLookup_node1728655079416, keys1=["do_locationid"], keys2=["dolocationid"], transformation_ctx="YellowTripsDataPickupZoneLookupDropoffZoneLookup_node1728658981088")

# Script generated for node Change Schema - Joined Data
ChangeSchemaJoinedData_node1728659330305 = ApplyMapping.apply(frame=YellowTripsDataPickupZoneLookupDropoffZoneLookup_node1728658981088, mappings=[("do_locationid", "long", "do_locationid", "long"), ("do_borough", "string", "do_borough", "string"), ("do_zone", "string", "do_zone", "string"), ("do_service_zone", "string", "do_service_zone", "string"), ("tpep_dropoff_datetime", "string", "dropoff_datetime", "timestamp"), ("extra", "double", "extra", "double"), ("trip_distance", "double", "trip_distance", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("mta_tax", "double", "mta_tax", "double"), ("congestion_surcharge", "double", "congestion_surcharge", "double"), ("ratecodeid", "long", "ratecodeid", "long"), ("total_amount", "double", "total_amount", "double"), ("payment_type", "long", "payment_type", "long"), ("pu_locationid", "long", "pu_locationid", "long"), ("fare_amount", "double", "fare_amount", "double"), ("vendorid", "long", "vendor_id", "long"), ("pu_zone", "string", "pu_zone", "string"), ("tpep_pickup_datetime", "string", "pickup_datetime", "timestamp"), ("pu_service_zone", "string", "pu_service_zone", "string"), ("pu_borough", "string", "pu_borough", "string"), ("tolls_amount", "double", "tolls_amount", "double"), ("tip_amount", "double", "tip_amount", "double"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("passenger_count", "long", "passenger_count", "long")], transformation_ctx="ChangeSchemaJoinedData_node1728659330305")

# Script generated for node Transformed Yellow Trip Data
TransformedYellowTripData_node1728659698852 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemaJoinedData_node1728659330305, connection_type="s3", format="glueparquet", connection_options={"path": "s3://serverlessanalytics-[your-id]-transformed/nyc-taxi/yellow-tripdata/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="TransformedYellowTripData_node1728659698852")

job.commit()
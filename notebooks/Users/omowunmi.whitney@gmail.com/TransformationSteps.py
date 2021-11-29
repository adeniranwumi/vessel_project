# Databricks notebook source
#mount source

dbutils.fs.mount(
  source = "wasbs://source@thynkvesselstorage.blob.core.windows.net",
  mount_point = "/mnt/Vessel_thynk",
  extra_configs = 
{"fs.azure.account.key.thynkvesselstorage.blob.core.windows.net":"YgO0/KpCtS8v03pHmh9azfOoCzC+G+R6OMeKz7+7Sqikl+OAy9hBj1IOaILUCHoNYvRg9RvmwS59PMutBpqv1w=="})

# COMMAND ----------

#connection to sql database

jdbcHostname = "vessel-project.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "Vessel_port_activity"
properties = {
    "user": "omowunmi@vessel-project",
    "password": "Pinkdisciple1",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

# COMMAND ----------

#import modules
import pyspark 
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import types as Types
from pyspark.sql import functions as Func
from pyspark.sql import DataFrameWriter as Writer
from math import radians, cos, sin, asin, sqrt
spark = (SparkSession.builder.appName("HDFS_Haversine_Fun").getOrCreate())


# COMMAND ----------

#read the files into data frames

vesselMove = spark.read.csv("/mnt/Vessel_thynk/VesselMovements.csv", inferSchema=True, header=True)

ports = spark.read.csv("/mnt/Vessel_thynk/Ports.csv", inferSchema=True, header=True)

vessels = spark.read.csv("/mnt/Vessel_thynk/Vessels.csv", inferSchema=True, header=True)

# COMMAND ----------

#remove duplicates from ports data frame
#I observed that some records had the same location data but different names and port keys so I removed them to prevent redundancy

ports = ports.dropDuplicates(["Latitude","Longitude"])

#rename columns 

ports = ports.withColumnRenamed("Latitude","port_Lat").withColumnRenamed("Longitude","port_Long").withColumnRenamed("PortSkey","Port_key").withColumnRenamed("PortName","Port_name")
vesselMove = vesselMove.withColumnRenamed("Lat","vessels_Lat").withColumnRenamed("Lng","vessels_Long")

# COMMAND ----------

#define function to calculate distances between ports and vessels using Haversine formula
def get_distance(longit_a, latit_a, longit_b, latit_b):

# Transform degrees to radians
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  latit_a, longit_b, latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a
    
    # Calculate area
    area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
    
    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    
    #the unit of the radius used is in nautical miles to ensure that the result is also in nautical miles
    radius = 3440.1
    
    # Calculate Distance
    distance = central_angle * radius
    return abs(round(distance, 2))  

udf_get_distance = Func.udf(get_distance)

# COMMAND ----------

#cross join vessels and ports into pairs

vessel_port_pairs = (vesselMove.crossJoin(ports).toDF(
"VesselIMO","VesselName","ToDate","vessels_Lat","vessels_Long",
"Port_key","Port_name","port_Lat","port_Long"))

# COMMAND ----------

#run udf_get_distance function on the port/vessel pairs and store the data in a new data frame

vessel_port_pairs_distance = vessel_port_pairs.withColumn("ABS_DISTANCE", 
udf_get_distance(vessel_port_pairs.vessels_Long, vessel_port_pairs.vessels_Lat,
vessel_port_pairs.port_Long, vessel_port_pairs.port_Lat).cast(Types.DoubleType()))

# COMMAND ----------

#the decided threshold to determine if a vessel visited a port is 1.5 nm
#store the port/vessels pairs <= 1.5 in a new data frame

visitedTrue = vessel_port_pairs_distance.filter(vessel_port_pairs_distance.ABS_DISTANCE <= 1.5)

# COMMAND ----------

#this calculates the time a vessel spent at the visited port
#NB: result might be inefficient because
#1. unable to get the difference in hours 
#2. unable to determine the time between port visitations when the vessel was at another location

visitDuration = visitedTrue.withColumn("duration", (Func.datediff(Func.to_timestamp(Func.col("ToDate"), 'd/M/y H:m'), Func.lag(Func.to_timestamp(Func.col("ToDate"), 'd/M/y H:m'), 1)
    .over(Window.partitionBy("VesselIMO","Port_key")
    .orderBy(Func.to_timestamp(Func.col("ToDate"), 'd/M/y H:m'))))))


# COMMAND ----------

#calculates sum of duration per port visitation

visitDuration = visitDuration.withColumn("TotalDuration", Func.sum(visitDuration.duration).over(Window.partitionBy("VesselIMO", "Port_key")))

#removes duplicates
disitinctVisitDuration = visitDuration.dropDuplicates(["VesselIMO","Port_key"])

#removes the column duration, no longer needed since duration has been aggregated
disitinctVisitDuration.drop("duration")

#replace null values in total duration column with 0
disitinctVisitDuration = disitinctVisitDuration.na.fill(value=0,subset=["TotalDuration"])
disitinctVisitDuration = disitinctVisitDuration.withColumnRenamed("ToDate","Arrival_date")

#drop unwanted columns
disitinctVisitDuration=disitinctVisitDuration.drop("duration","vessels_Lat","vessels_Long","Port_name","VesselName","port_Lat","port_Long","ABS_DISTANCE")

# COMMAND ----------

#Transform data to be loaded into sql DB

#data frame for unique ports
ports_load = ports.select("Port_key","Port_name")

#data frame for unique vessel types
vesselTypes = vessels.drop("InternationalMaritimeOrganizationNumber","VesselStatusName",
                          "DeadWeightTonnage","Speed","ManagementcompanyCode","ManagementCompany").dropDuplicates()

#data frame for unique vessel status
vesselStatus = vessels.drop("InternationalMaritimeOrganizationNumber","VesselTypeName",
                          "DeadWeightTonnage","Speed","ManagementcompanyCode","ManagementCompany").dropDuplicates()

#data frame for unique management companies
mgmtCompany = vessels.drop("InternationalMaritimeOrganizationNumber","VesselTypeName",
                          "DeadWeightTonnage","Speed","VesselStatusName","VesselTypeName").dropDuplicates()

##data frame for unique vessel names
vesselNames = vesselMove.drop("ToDate","vessels_Lat","vessels_Long").dropDuplicates()

#data frame for complete vessel details
vesselInfo = vesselNames.join(vessels,vesselNames.VesselIMO == vessels.InternationalMaritimeOrganizationNumber, "inner")
vesselInfo = vesselInfo.drop("InternationalMaritimeOrganizationNumber","ManagementCompany")

# COMMAND ----------

ports_load = Writer(ports_load)
vesselTypes = Writer(vesselTypes)
vesselStatus = Writer(vesselStatus)
mgmtCompany = Writer(mgmtCompany)
vesselInfo = Writer(vesselInfo)
port_visitation = Writer(disitinctVisitDuration)

# COMMAND ----------

ports_load.jdbc(url=url, table="PORTS", mode="append", properties=properties)
vesselTypes.jdbc(url=url, table="VESSEL_TYPES",mode="append", properties=properties)
vesselStatus.jdbc(url=url, table="VESSEL_STATUS", mode="append",properties=properties)
mgmtCompany.jdbc(url=url, table="MANAGEMENT_COMPANY", mode="append", properties=properties)
vesselInfo.jdbc(url=url, table="VESSEL_INFO", mode="append", properties=properties)
port_visitation.jdbc(url=url, table="PORT_VISITATION", mode="append", properties=properties)
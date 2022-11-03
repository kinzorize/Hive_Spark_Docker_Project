import pyspark
from pyspark.sql import SparkSession

# create spark session and Hive metastore property
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
SparkContext.setSystemProperty(
    "hive.metastore.uris", "thrift://52.206.224.230:9083/")
sparkSession = (SparkSession
                .builder
                .appName('integration-pyspark-hive1')
                .enableHiveSupport()
                .getOrCreate())

sparkSession.sql('show databases').show()

sparkSession.sql('create database airline_flight').show()

sparkSession.sql("use airline_flight").show()
sparkSession.sql("show tables").show()

# create spark datafrme from csv file
AirlineDF = sparkSession.read.option("header", "true").csv(
    "/Users/ghost/Documents/airlines1.csv")

AirlineDF.show()

AirlineDF1 = AirlineDF.select("Year", "Reporting_Airline")

AirlineDF1.show()

AirlineDF1.write.saveAsTable("airline_flight.airline")

sparkSession.sql("use airline_flight").show()
sparkSession.sql("show tables").show()

sparkSession.sql("select count(*) from airline").show()

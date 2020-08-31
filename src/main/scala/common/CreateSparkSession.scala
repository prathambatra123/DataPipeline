package common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object CreateSparkSession {

  private val Logger = LoggerFactory.getLogger(getClass.getName)

  def sparkSession():Option[SparkSession] ={
    Logger.info("Spark Session Creation Started")
    try {
      Logger.info("Setting Hadoop Home")
      //System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession.builder.appName("PFPSDatamart").config("spark.master","yarn")
               .config("spark.sql.warehouse.dir","/apps/hive/warehouse/")
                .enableHiveSupport.getOrCreate()

      val sampleSeq=Seq((1,"spark"))
      val sampleDf = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
      Some(spark)
    }
    catch {
      case e:Exception =>
        Logger.error("Error occured in creating Spark Session" + e.printStackTrace())
        None
    }
  }

}

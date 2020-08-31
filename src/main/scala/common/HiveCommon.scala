package common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object HiveCommon {
  private val Logger = LoggerFactory.getLogger(getClass.getName)

  def readFromHive(spark : SparkSession ,tblNm : String,query : String ) : Option[DataFrame] = {
    try {
  Logger.info("Reading from Hive Table" + tblNm)
    val df= spark.sql(query)
    Some(df)
    } catch {
      case e:Exception=>
        Logger.error("Error reading from table "+tblNm+e.printStackTrace())
        None
    }
  }

  def writeToHive(spark : SparkSession,df : DataFrame,tgtTbl : String) = {
    Logger.info("write to HIve table started for "+tgtTbl)
    try {
    df.createOrReplaceTempView("tmpview")
      Logger.info(tgtTbl+"_view  created")
      spark.sql("use retail_db")
      Logger.info("use statement executed")
      spark.sql("insert into "+tgtTbl + "select * from tmpview)")
    Logger.info("write to HIve table ended for "+tgtTbl)
  } catch {
      case e:Exception=>
        Logger.error("Error occured while writing to Hive table "+tgtTbl + e.printStackTrace())
    }
  }

}

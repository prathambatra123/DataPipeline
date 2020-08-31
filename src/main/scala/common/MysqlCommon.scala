package common

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object MysqlCommon {
  private val Logger = LoggerFactory.getLogger(getClass.getName)
    def mySqlProperties() : Properties ={
      Logger.info("Entering MYSQL properties")
      val pgProperties = new Properties()
      pgProperties.put("user","retail_dba")
      pgProperties.put("password","itversity")
      Logger.info("MYSQL properties Ended")
      pgProperties
    }

    val mySqlUrl="jdbc:MYSQL://localhost:3306/retail_db"

    def readFromMySql(spark : SparkSession,mySqlProp : Properties,tblNm : String) : Option[DataFrame] ={
      Logger.info("Reading from Mysql Started "+tblNm)
      try {
        val mySqlUrl="jdbc:MYSQL://localhost:3306/retail_db"
        val df = spark.read.option("driver","com.mysql.jdbc.Driver").jdbc(mySqlUrl,tblNm,mySqlProp)
        Logger.info("Reading from MYSQL Ended"+tblNm)
        Some(df)
      }
      catch {
        case e:Exception =>
          Logger.error("Error Reading from Mysql table "+tblNm + e.printStackTrace())
          System.exit(1)
          None

            }
           }

    def writeToMySql(spark : SparkSession,mySqlProp : Properties,df : DataFrame,tgtTbl :  String): Unit = {
      Logger.info("Writing to MYSQL Started" + tgtTbl)
      try {
        df.write.mode(SaveMode.Append).format("jdbc").option("url",mySqlUrl)
          .option("dbtable",tgtTbl)
          .option("user","retail_dba")
          .option("password","itversity")
          .save()
        Logger.info("Writing to MYSQL Ended" +tgtTbl)
          } catch {
         case e:Exception =>
          Logger.error("Error Reading from Mysql table "+tgtTbl + e.printStackTrace())
      }
    }

}

package common

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.slf4j.LoggerFactory.getLogger

object PostgresCommon {
private val Logger = LoggerFactory.getLogger(getClass.getName)

  def postgresProperties() : Properties ={
    Logger.info("Entering PG properties")
    val pgProperties = new Properties()
    pgProperties.put("user","postgres")
    pgProperties.put("password","admin")
    Logger.info("PG properties Ended")
    pgProperties
  }

  val Pgurl="jdbc:postgresql://localhost:5432/futurex"

  def readFromPostgres(spark : SparkSession,pgProp : Properties,tblNm : String) : Option[DataFrame] ={
    Logger.info("Reading from Postgres Started "+tblNm)
    try {
      val Pgurl="jdbc:postgresql://localhost:5432/futurex"
    val df = spark.read.jdbc(Pgurl,tblNm,pgProp)
      Logger.info("Reading from Postgres Ended "+tblNm)
      Some(df)
       }
    catch {
      case e:Exception =>
        Logger.error("Error Reading from Postgres "+tblNm + e.printStackTrace())
        None
    }
  }

  def wrtieToPostgres(spark : SparkSession,pgProp : Properties,df : DataFrame,tgtTbl :  String): Unit = {
    Logger.info("Writing to Postgres Started "+tgtTbl)
    try {
      df.write.mode(SaveMode.Append).format("jdbc").option("url",Pgurl)
              .option("dbtable",tgtTbl)
              .option("user","postgres")
              .option("password","admin")
              .save()
      Logger.info("Writing to Postgres Ended "+tgtTbl)
         } catch {
      case e:Exception =>
        Logger.error("Error Reading from Postgres "+tgtTbl + e.printStackTrace())
         }
  }
}

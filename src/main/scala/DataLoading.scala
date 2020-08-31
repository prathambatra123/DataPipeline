import common.{CreateSparkSession, HiveCommon, MysqlCommon, PostgresCommon}
import org.slf4j.LoggerFactory
import java.io.{File, FileInputStream}
import java.util

import org.yaml.snakeyaml.Yaml
object DataLoading {
  private val Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try{
      val arg_len = args(0).length
      if (arg_len == 0) {
        Logger.warn("No YAML argument passed")
        System.exit(1)
      }
      val ymlFile = args(0)
      val spark = CreateSparkSession.sparkSession().get
      val ios = new FileInputStream(new File(ymlFile))
      val yaml = new Yaml()
      val obj: util.Map[String, Any] = yaml.load(ios).asInstanceOf[java.util.Map[String, Any]]
      val src_tbl = obj.get("source").toString
      val tgt_tbl = obj.get("target").toString
      val tgt_query = obj.get("sql").toString

      //val df = PostgresCommon.readFromPostgres(spark,PostgresCommon.postgresProperties(),src_tbl).get
      val df = HiveCommon.readFromHive(spark,src_tbl,tgt_query).get
      HiveCommon.writeToHive(spark,df,tgt_tbl)
    } catch {
      case e:Exception=>
        Logger.error("error in main method "+e.printStackTrace())
        System.exit(1)
    }
    }
}

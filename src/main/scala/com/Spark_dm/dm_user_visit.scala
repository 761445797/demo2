package com.Spark_dm
import com.Constants.Constan
import com.SparkUtils.JDBCUtils
import com.config.ConfigManager
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object dm_user_visit {



    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName(Constan.SPARK_APP_NAME_USER).setMaster(Constan.SPARK_LOACL)
      val sc = new SparkContext(conf)
      val hiveContext = new HiveContext(sc)
      // 加载相应的语句
      val sql = ConfigManager.getProper(args(0))
      if(sql == null){
        LoggerFactory.getLogger("SparkLogger")
          .debug("提交的表名参数有问题！请重新设置。。。")
      }else{
        //val finalSql = sql.replace("?",args(1))
        // 运行SQL
        val df: DataFrame = hiveContext.sql(sql)
        // 处理配置参数
        val mysqlTableName = args(0).split("\\.")(1)
        val hiveTableName = args(0)
        val jdbcProp = JDBCUtils.getJdbcProp()._1
        val jdbcUrl = JDBCUtils.getJdbcProp()._2
        df.show()

        // 存入MySQL
        //df.write.mode(SaveMode.Append).jdbc(jdbcUrl,mysqlTableName,jdbcProp)
        // 存入Hive
        // df.write.mode(SaveMode.Overwrite).insertInto(hiveTableName)
      }
    }


}

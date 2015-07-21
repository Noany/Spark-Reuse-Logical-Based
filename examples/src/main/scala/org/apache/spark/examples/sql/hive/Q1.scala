package org.apache.spark.examples.sql.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zengdan on 15-4-22.
 */
object Q1 {
  def main(args: Array[String]) {

    /*
    val t = new Thread{
      override def run() = QGMaster.main("--host localhost --port 7070".split(" "))
    }
    t.start()

    Thread.sleep(5000)
    */


    System.setProperty("spark.sql.auto.cache", "false")
    System.setProperty("spark.sql.autoBroadcastJoinThreshold","0")
    System.setProperty("spark.sql.shuffle.partitions","2")
    System.setProperty("hive.metastore.warehouse.dir", "/Users/zengdan/hive")
    val conf = new SparkConf()
    conf.setAppName("Test Q2")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.tachyon.impl","tachyon.hadoop.TFS")


    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    //for(i <- 1 to 1)
    //  executeQ2(sqlContext)

    sc.stop()
    //sqlContext.stop()

  }

  /*
  def executeQ2(sqlContext: HiveContext){
    val res0 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16'
                                group by l_returnflag, l_linestatus""")
    res0.foreach(println)
    //println(s"Result count is ${res0.count()}")
  }
  */
}

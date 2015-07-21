package org.apache.spark.examples.sql.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.auto.cache.QGMaster

/**
 * Created by zengdan on 15-4-21.
 */
object Q3 {
  def main(args: Array[String]) {

    ///*
    val t = new Thread{
      override def run() = QGMaster.main("--host localhost --port 7070".split(" "))
    }
    t.start()

    Thread.sleep(5000)
    //*/

    System.setProperty("spark.sql.auto.cache", "true")
    System.setProperty("spark.sql.shuffle.partitions","2")
    System.setProperty("hive.metastore.warehouse.dir", "/Users/zengdan/hive")
    val conf = new SparkConf()
    conf.setAppName("Test Q2")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.tachyon.impl","tachyon.hadoop.TFS")


    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

   for(i <- 1 to 2)
    executeQ2(sqlContext)

    sc.stop()
    sqlContext.stop()

  }

  def executeQ2(sqlContext: HiveContext) {

    val query = """select l_orderkey,sum(l_extendedprice * (1 - l_discount)) as revenue,o_orderdate,o_shippriority
                                from customer,orders,lineitem
                                where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-22' and l_shipdate > '1995-03-22'
                                group by l_orderkey,o_orderdate,o_shippriority"""

    val res0 = sqlContext.sql(query).count()
    println(s"Result length is ${res0}")
  }
}

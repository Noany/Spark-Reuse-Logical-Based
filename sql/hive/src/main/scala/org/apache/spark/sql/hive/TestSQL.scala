package org.apache.spark.sql.hive

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zengdan on 15-4-29.
 */
object TestSQL {
  def main(args: Array[String]) {
    System.setProperty("spark.sql.auto.cache", "true")
    System.setProperty("spark.sql.shuffle.partitions","2")
    System.setProperty("hive.metastore.warehouse.dir", "/Users/zengdan/hive")
    val conf = new SparkConf()
    conf.setAppName("TPCH Q1").setMaster("local")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.tachyon.impl","tachyon.hadoop.TFS")

    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val path = "hdfs://localhost:9000/user/zengdan/tpch"

    for(i <- 1 to 2) {
      val res1 = sqlContext.sql("""
                                select l_orderkey,sum(l_quantity) as t_sum_quantity
                                from lineitem
                                where l_orderkey is not null
                                group by l_orderkey""")
      res1.persist(StorageLevel.MEMORY_AND_DISK).registerTempTable("q18_tmp_cached")

      val res2 = sqlContext.sql("""select c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum(l_quantity)
                                from customer,orders,q18_tmp_cached t,lineitem l
                                where c_custkey = o_custkey and o_orderkey = t.l_orderkey and
                                o_orderkey is not null and t.t_sum_quantity > 300 and o_orderkey = l.l_orderkey
                                and l.l_orderkey is not null
                                group by c_name, c_custkey,o_orderkey,o_orderdate,o_totalprice""")
      res2.collect().foreach(println)
      //println(res2.count())
    }
  }
}

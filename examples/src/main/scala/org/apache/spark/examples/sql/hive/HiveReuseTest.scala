package org.apache.spark.examples.sql.hive

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.auto.cache.QGMaster
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zengdan on 15-4-2.
 */
object HiveReuseTest {
  def main(args: Array[String]) {

    /*
    val t = new Thread{
      override def run() = QGMaster.main("--host localhost --port 7070".split(" "))
    }
    t.start()

    Thread.sleep(5000)
    */

    System.setProperty("spark.sql.auto.cache", "true")
    System.setProperty("spark.sql.autoBroadcastJoinThreshold","0")
    System.setProperty("spark.sql.shuffle.partitions","2")
    System.setProperty("hive.metastore.warehouse.dir", "/Users/zengdan/hive")
    val conf = new SparkConf()
    conf.setAppName("Test Q2")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.tachyon.impl","tachyon.hadoop.TFS")


    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    for(i <- 1 to 1)
      executeQ2(sqlContext)

    sc.stop()
    //sqlContext.stop()

  }

  def executeQ2(sqlContext: HiveContext){

    /*
    val query = """
                  | select s.s_acctbal as s_acctbal, s.s_name as s_name,
                  |n.n_name as n_name,
                  |p.p_partkey as p_partkey,
                  |ps.ps_supplycost as ps_supplycost,
                  |p.p_mfgr as p_mfgr,
                  |s.s_address as s_address, s.s_phone as s_phone, s.s_comment as s_comment
                  |from nation n join region r on n.n_regionkey = r.r_regionkey and r.r_name = 'EUROPE'
                  |join supplier s on s.s_nationkey = n.n_nationkey
                  |join partsupp ps on s.s_suppkey = ps.ps_suppkey
                  |join part p on p.p_partkey = ps.ps_partkey and p.p_size = 15 and p.p_type like '%BRASS' """.stripMargin

    val res0 = sqlContext.sql(query).persist(StorageLevel.MEMORY_AND_DISK)
    //val result = res0.collect()
    //println(s"Result count is ${result.length}")
    //res0.collect()
    //println(res0.queryExecution.optimizedPlanOriginal(0))

    res0.registerTempTable("tmp1")



    //delete min,  group by p_partkey
    val res1 = sqlContext.sql(" select p_partkey, min(ps_supplycost) as ps_min_supplycost from tmp1 group by p_partkey")
    res1.registerTempTable("tmp2")

    //val res2 = sqlContext.sql("select * from tmp2 join tmp1 on tmp2.p_partkey=tmp1.p_partkey")
    val res2 = sqlContext.sql("select t1.s_acctbal, t1.s_name, t1.n_name, t1.p_partkey, t1.p_mfgr, t1.s_address, " +
      "t1.s_phone, t1.s_comment from tmp1  t1 " +
      "join tmp2 t2 on t1.p_partkey = t2.p_partkey " +
      "and t1.ps_supplycost=t2.ps_min_supplycost ")
    //"order by s_acctbal desc, n_name, s_name, p_partkey")
    */
    val res0 = sqlContext.sql("""select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
                                from lineitem
                                where l_shipdate <= '1998-09-16'
                                group by l_returnflag, l_linestatus""")
    res0.foreach(println)
    //println(s"Result count is ${res0.count()}")
  }

}

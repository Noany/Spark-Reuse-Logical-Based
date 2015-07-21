package org.apache.spark.examples.sql.hive

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by zengdan on 15-4-5.
 */
object CreateTB {
  def main(args: Array[String]) {
    System.setProperty("spark.sql.shuffle.partitions","2")
    System.setProperty("hive.metastore.warehouse.dir", "/Users/zengdan/hive")
    val conf = new SparkConf()
    conf.setAppName("Create Table")
    val sc = new SparkContext(conf)


    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //sqlContext.setConf("hive.metastore.warehouse.dir","/Users/zengdan/hive")

    val lineitem = sqlContext.sql("create external table if not exists  lineitem (l_orderkey int, l_partkey int, l_suppkey int, " +
      "l_linenumber int, l_quantity double, l_extendedprice double, l_discount double, l_tax double, l_returnflag string," +
      " l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string, l_shipinstruct string, " +
      "l_shipmode string, l_comment string) row format delimited fields terminated by '|' stored as textfile location '"
      +args(0)+"/lineitem'")
    lineitem.count()
    println("table lineitem is successfully created!")

    val part = sqlContext.sql("create external table if not exists part (p_partkey int, p_name string, p_mfgr string, p_brand string, p_type string, p_size int, p_container string, p_retailprice double, p_comment string) row format delimited fields terminated by '|' stored as textfile location '"+args(0)+"/part'")
    part.count()
    println("table part is successfully created!")

    val supplier = sqlContext.sql("create external table if not exists supplier (s_suppkey int, s_name string, s_address string, s_nationkey int, s_phone string, s_acctbal double, s_comment string) row format delimited fields terminated by '|' stored as textfile location '" + args(0) + "/supplier'")
    supplier.count()
    println("table supplier is successfully created!")

    val partsupp = sqlContext.sql("create external table if not exists partsupp (ps_partkey int, ps_suppkey int, ps_availqty int, ps_supplycost double, ps_comment string) row format delimited fields terminated by '|' stored as  textfile location '" + args(0) + "/partsupp'")
    partsupp.count()
    println("table partsupp is successfully created!")

    val nation = sqlContext.sql("create external table if not exists nation (n_nationkey int, n_name string, n_regionkey int, n_comment string) row format delimited fields terminated by '|' stored as textfile location '" + args(0) + "/nation'")
    nation.count()
    println("table nation is successfully created!")

    val region = sqlContext.sql("create external table if not exists region (r_regionkey int, r_name string, r_comment string) row format delimited fields terminated by '|' stored as textfile location '" + args(0) + "/region'")
    region.count()
    println("table region is successfully created!")

    val orders = sqlContext.sql("create external table if not exists orders (o_orderkey int, o_custkey int, o_orderstatus string, o_totalprice double, o_orderdate string, o_orderpriority string, o_clerk string, o_shippriority int, o_comment string) row format delimited fields terminated by '|' stored as textfile location '" + args(0) + "/orders'")
    orders.count()
    println("table orders is successfully created!")

    val customer = sqlContext.sql("create external table if not exists customer (c_custkey int, c_name string, c_address string, c_nationkey int, c_phone string, c_acctbal double, c_mktsegment string, c_comment string) row format delimited fields terminated by '|' stored as textfile location '" + args(0) + "/customer'")
    customer.count()
    println("table customer is successfully created!")

  }
}

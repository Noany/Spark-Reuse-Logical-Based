/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.execution.SparkPlan

import java.util.HashMap


case class Lineitem(L_ORDERKEY: Int, L_PARTKEY: Int, L_SUPPKEY: Int, L_LINENUMBER: Int,
                    L_QUANTITY: Double, L_EXTENDEDPRICE: Double, L_DISCOUNT: Double, L_TAX: Double,
                    L_RETURNFLAG: String, L_LINESTATUS: String, L_SHIPDATE: String, L_COMMITDATE: String,
                    L_RECEIPTDATE: String, L_SHIPINSTRUCT: String, L_SHIPMODE: String, L_COMMENT: String)

case class Orders(O_ORDERKEY: Int, O_CUSTKEY: Int, O_ORDERSTATUS: String, O_TOTALPRICE: Double,
                  O_ORDERDATE: String, O_ORDERPRIORITY: String, O_CLERK: String, O_SHIPPRIORITY: Int,
                  O_COMMENT: String)

object PushTest {
  def main(args: Array[String]) {

    var start = System.nanoTime
    //System.setProperty("spark.push.mode", args(1));
    //System.setProperty("spark.array.mode", args(2))
    //System.setProperty("spark.sql.cost.estimate", "true")
    val sparkConf = new SparkConf()
    //sparkConf.set("spark.push.mode", args(1))

    val sc = new SparkContext(sparkConf.setAppName("SQL Testing"))

    val sqlContext = new SQLContext(sc)

    import sqlContext._

    val file1 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/lineitem.tbl")
    //val file2 = sqlContext.sparkContext.textFile(args(1))
    //file1.cache()
    //file1.persist(StorageLevel.MEMORY_ONLY)
    //file1.count()

    val table_lineitem = file1.map(_.split('|')).map(l => Lineitem(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toInt,
      l(4).toDouble, l(5).toDouble, l(6).toDouble, l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15)))

    //val table_orders = file2.map(_.split("\\|")).map(l => Orders(l(0).toInt, l(1).toInt, l(2), l(3).toDouble,
    //  l(4), l(5), l(6), l(7).toInt, l(8)))
    val tb = sqlContext.createSchemaRDD[Lineitem](table_lineitem)
    tb.registerTempTable("lineitem")
    //tb.cache()

    //table_lineitem.registerTempTable("lineitem")
    //table_orders.registerTempTable("orders")

    val query = "SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), " +
      "SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)), SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)), " +
      "AVG(L_QUANTITY), AVG(L_EXTENDEDPRICE), AVG(L_DISCOUNT), COUNT(1) FROM lineitem WHERE L_SHIPDATE < '1993-06-27' " +
      "GROUP BY L_RETURNFLAG, L_LINESTATUS  "
    val query2 = "SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), " +
      "SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)), SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)), " +
      "AVG(L_QUANTITY), AVG(L_EXTENDEDPRICE), AVG(L_DISCOUNT), COUNT(1) FROM lineitem WHERE L_SHIPDATE < '1993-08-27' " +
      "GROUP BY L_RETURNFLAG, L_LINESTATUS  "
    val rdd = sql(query)
    val rdd2 = sql(query2)
    //ORDER BY L_RETURNFLAG, L_LINESTATUS
    println("====Time used before sql is %f s====".format((System.nanoTime - start) / 1e9))


    rdd.cache()
    rdd2.collect()
    //rdd.collect()

    start = System.nanoTime()

    //val times = new HashMap[String, Long]
    //val plan:SparkPlan = rdd.queryExecution.executedPlan
    //plan.executeAndCount(times).collect()
    //println(plan.toString)

    //file1.asInstanceOf[SchemaRDD]
    //sc.dagScheduler.stats
    //rdd.queryExecution.executedPlan.foreach(println)
    //rdd.collect()
  }

}


package org.apache.spark.util

/**
 * Created by zengdan on 15-1-21.
 */
import java.util.HashMap
import scala.collection.mutable.Map

object Stats {
  //val statistics = new ThreadLocal[HashMap[Int, Array[Long]]]()
  val statistics = new ThreadLocal[Map[Int, Array[Int]]]()
  //time, rowcount, averagesize
  //val rowCountByOperator = new ThreadLocal[HashMap[String, Long]]()
  //val sizeByOperator = new ThreadLocal[HashMap[String, Long]]()
}

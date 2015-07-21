package org.apache.spark.sql.auto.cache

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.SerializableBuffer

import scala.collection.mutable.HashMap
import java.nio.ByteBuffer

/**
 * Created by zengdan on 15-3-25.
 */
sealed trait QGUtils extends Serializable

private object QGUtils{
  case class PlanDesc(jars: HashMap[String, Long], serializedPlan: SerializableBuffer)
}

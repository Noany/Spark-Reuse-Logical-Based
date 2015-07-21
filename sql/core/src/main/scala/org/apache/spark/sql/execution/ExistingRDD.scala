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

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{StructType, SQLContext}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Row, Attribute, GenericMutableRow}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics, QNodeRef}
import org.apache.spark.sql.catalyst.types.NativeType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Stats

import scala.collection.mutable.Map

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
object RDDConversions {

  def productToRowRdd[A <: Product](data: RDD[A], schema: StructType): RDD[Row] = {
    data.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator.empty
      } else {
        val bufferedIterator = iterator.buffered
        val mutableRow = new GenericMutableRow(bufferedIterator.head.productArity)
        val schemaFields = schema.fields.toArray
        bufferedIterator.map { r =>
           var i = 0
           while (i < mutableRow.length) {
             mutableRow(i) =
               ScalaReflection.convertToCatalyst(r.productElement(i), schemaFields(i).dataType)
             i += 1
           }
           mutableRow
        }
      }
    }
  }
}

case class LogicalRDD(output: Seq[Attribute], rdd: RDD[Row])(sqlContext: SQLContext)
  extends LogicalPlan with MultiInstanceRelation {

  def children = Nil

  def newInstance() =
    LogicalRDD(output.map(_.newInstance()), rdd)(sqlContext).asInstanceOf[this.type]

  override def sameResult(plan: LogicalPlan) = plan match {
    case LogicalRDD(_, otherRDD) => rdd.id == otherRDD.id
    case _ => false
  }

  override def operatorMatch(plan: LogicalPlan) = this.sameResult(plan)

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(sqlContext.defaultSizeInBytes)
  )
}

case class PhysicalRDD(output: Seq[Attribute], rdd: RDD[Row], optionRef: Option[QNodeRef] = None) extends LeafNode {
  //this.id = optionId
  this.nodeRef = optionRef
  override def execute() = {
    if(!nodeRef.isDefined){
      rdd
    }else{
      var newRdd = if(!nodeRef.get.collect){
        rdd
      }else{
        var fixedSize = 0
        var varIndex: List[Int] = Nil

        var i = 0
        while(i < output.length){
          if (output(i).dataType.isInstanceOf[NativeType]) {
            var tp = NativeType.defaultSizeOf(output(i).dataType.asInstanceOf[NativeType])
            if (tp == 4096) {
              varIndex = varIndex ::: List(i)
            } else {
              fixedSize += tp
            }
          }
          i += 1
        }

        rdd.mapPartitions{iter =>
          var statistics = Stats.statistics.get()
          if(statistics == null) {  //no match in the query graph, so no need to collect information
            statistics = Map[Int, Array[Int]]()
            Stats.statistics.set(statistics)
          }

          new Iterator[Row]{
            override def hasNext: Boolean = {
              val start = System.nanoTime()
              val continue = iter.hasNext
              time += (System.nanoTime() - start)
              if(!continue && rowCount != 0 && nodeRef.get.collect){
                avgSize = (fixedSize + avgSize/rowCount)
                println(s"ExistingRDD: $time, $rowCount, $avgSize")
                statistics.put(nodeRef.get.id, Array((time/1e6).toInt, avgSize*rowCount))
              }
              continue
            }

            override def next = {
              val start = System.nanoTime()
              val mutableRow = iter.next()

              time += (System.nanoTime() - start)
              rowCount += 1
              for(index <- varIndex){
                avgSize += mutableRow.getString(index).length
              }
              mutableRow
            }
          }
        }
      }

      if(nodeRef.get.cache) {
        newRdd.cacheID = Some(nodeRef.get.id)
        //newRdd = newRdd.sparkContext.saveAndLoadOperatorFile[Row](newRdd.cacheID.get,
        //  newRdd.map(ScalaReflection.convertRowToScala(_, schema)))
        newRdd = SQLContext.cacheData(newRdd, output, nodeRef.get.id)
        //newRdd.persist(StorageLevel.OFF_HEAP)
        //rdd.clearGlobalDependencies()
      }
      newRdd
    }
  }
}

@deprecated("Use LogicalRDD", "1.2.0")
case class ExistingRdd(output: Seq[Attribute], rdd: RDD[Row]) extends LeafNode {
  override def execute() = rdd
}

@deprecated("Use LogicalRDD", "1.2.0")
case class SparkLogicalPlan(alreadyPlanned: SparkPlan)(@transient sqlContext: SQLContext)
  extends LogicalPlan with MultiInstanceRelation {

  def output = alreadyPlanned.output
  override def children = Nil

  override final def newInstance(): this.type = {
    SparkLogicalPlan(
      alreadyPlanned match {
        case ExistingRdd(output, rdd) => ExistingRdd(output.map(_.newInstance()), rdd)
        case _ => sys.error("Multiple instance of the same relation detected.")
      })(sqlContext).asInstanceOf[this.type]
  }

  override def sameResult(plan: LogicalPlan) = plan match {
    case SparkLogicalPlan(ExistingRdd(_, rdd)) =>
      rdd.id == alreadyPlanned.asInstanceOf[ExistingRdd].rdd.id
    case _ => false
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(sqlContext.defaultSizeInBytes)
  )
}

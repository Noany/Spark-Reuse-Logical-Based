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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.{Row, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

/**
 * :: DeveloperApi ::
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
@DeveloperApi
case class BroadcastHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan,
    optionRef: Option[QNodeRef] = None)
  extends BinaryNode with HashJoin {

  this.nodeRef = optionRef

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  //zengdan add lazy
  @transient
  private lazy val broadcastFuture = future {
    // Note that we use .execute().collect() because we don't want to convert data to Scala types
    val input: Array[Row] = buildPlan.execute().map(_.copy()).collect()
    val hashed = if(nodeRef.isDefined && nodeRef.get.collect) {
      HashedRelation(input.iterator, buildSideKeyGenerator, input.length)
    }else{
      val (relation, ptime) = HashedRelation.createRelation(input.iterator, buildSideKeyGenerator, input.length)
      time += ptime
      relation
    }
    val start = System.nanoTime()
    val result = sparkContext.broadcast(hashed)
    time += (System.nanoTime() - start)
    result
  }

  override def execute() = {
    val broadcastRelation = Await.result(broadcastFuture, 5.minute)

    lazy val old = streamedPlan.execute().mapPartitions { streamedIter =>
      hashJoin(streamedIter, broadcastRelation.value)
    }
    if(!nodeRef.isDefined){
      old
    }else {
      var newRdd = if (!nodeRef.get.collect) {
        old
      } else {
        streamedPlan.execute().mapPartitions { streamedIter =>
          hashJoinWithCollect(streamedIter, broadcastRelation.value)
        }
      }

      if (nodeRef.get.cache) {
        newRdd.cacheID = Some(nodeRef.get.id)
        newRdd = SQLContext.cacheData(newRdd, output, nodeRef.get.id)
        //newRdd = newRdd.sparkContext.saveAndLoadOperatorFile[Row](newRdd.cacheID.get,
        //  newRdd.map(ScalaReflection.convertRowToScala(_, schema)))
      }
      newRdd
    }
  }
}

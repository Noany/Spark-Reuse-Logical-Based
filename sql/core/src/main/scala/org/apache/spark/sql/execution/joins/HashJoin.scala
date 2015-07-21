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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.Stats
import org.apache.spark.util.collection.CompactBuffer
import scala.collection.mutable.Map

trait HashJoin {
  self: SparkPlan =>

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val buildSide: BuildSide
  val left: SparkPlan
  val right: SparkPlan

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  override def output = left.output ++ right.output

  //zengdan
  @transient lazy val (fixedSize, stringIndexes) = outputSize(output)

  @transient protected lazy val buildSideKeyGenerator: Projection =
    newProjection(buildKeys, buildPlan.output)

  @transient protected lazy val streamSideKeyGenerator: () => MutableProjection =
    newMutableProjection(streamedKeys, streamedPlan.output)

  //zengdan
  protected def hashJoinWithCollect(streamIter: Iterator[Row], hashedRelation: HashedRelation): Iterator[Row] =
  {
    new Iterator[Row] {
      val start = System.nanoTime()
      var parentReadTime = 0L
      private[this] var currentStreamedRow: Row = _
      private[this] var currentHashMatches: CompactBuffer[Row] = _
      private[this] var currentMatchPosition: Int = -1

      // Mutable per row objects.
      private[this] val joinRow = new JoinedRow2

      private[this] val joinKeys = streamSideKeyGenerator()
      time += (System.nanoTime()-start)

      override final def hasNext: Boolean = {
        //val start = System.nanoTime()
        val continue = (currentMatchPosition != -1 && currentMatchPosition < currentHashMatches.size) ||
          (streamIter.hasNext && fetchNextWithCollect())
        //parentReadTime += (System.nanoTime()-start)

        if (!continue && rowCount != 0 && nodeRef.get.collect) {
          avgSize = (fixedSize + avgSize/rowCount)
          time += parentReadTime
          logDebug(s"HashJoin: $time, $rowCount, $avgSize")
          var statistics = Stats.statistics.get()
          if (statistics == null) {
            statistics = Map[Int, Array[Int]]()
            Stats.statistics.set(statistics)
          }

          statistics.put(nodeRef.get.id, Array((time/1e6).toInt, avgSize*rowCount))
          time = parentReadTime
        }
        continue
      }

      override final def next() = {
        val start = System.nanoTime()
        val ret = buildSide match {
          case BuildRight => joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
          case BuildLeft => joinRow(currentHashMatches(currentMatchPosition), currentStreamedRow)
        }
        currentMatchPosition += 1
        parentReadTime += (System.nanoTime() - start)
        rowCount += 1
        for (index <- stringIndexes) {
          //sizeInBytes += result.getString(index).length
          avgSize += ret.getString(index).length
        }
        ret
      }

      /**
       * Searches the streamed iterator for the next row that has at least one match in hashtable.
       *
       * @return true if the search is successful, and false if the streamed iterator runs out of
       *         tuples.
       */
      private final def fetchNext(): Boolean = {
        currentHashMatches = null
        currentMatchPosition = -1

        while (currentHashMatches == null && streamIter.hasNext) {
          currentStreamedRow = streamIter.next()
          if (!joinKeys(currentStreamedRow).anyNull) {
            currentHashMatches = hashedRelation.get(joinKeys.currentValue)
          }
        }

        if (currentHashMatches == null) {
          false
        } else {
          currentMatchPosition = 0
          true
        }
      }
      private final def fetchNextWithCollect(): Boolean = {
        currentHashMatches = null
        currentMatchPosition = -1

        var start = 0L
        while (currentHashMatches == null && streamIter.hasNext) {
          currentStreamedRow = streamIter.next()
          start = System.nanoTime()
          if (!joinKeys(currentStreamedRow).anyNull) {
            currentHashMatches = hashedRelation.get(joinKeys.currentValue)
          }
          time += (System.nanoTime() - start)
        }

        if (currentHashMatches == null) {
          false
        } else {
          currentMatchPosition = 0
          true
        }
      }
    }
  }

  protected def hashJoin(streamIter: Iterator[Row], hashedRelation: HashedRelation): Iterator[Row] =
  {
    new Iterator[Row] {
      private[this] var currentStreamedRow: Row = _
      private[this] var currentHashMatches: CompactBuffer[Row] = _
      private[this] var currentMatchPosition: Int = -1

      // Mutable per row objects.
      private[this] val joinRow = new JoinedRow2(null, null)

      private[this] val joinKeys = streamSideKeyGenerator()

      override final def hasNext: Boolean = {
        (currentMatchPosition != -1 && currentMatchPosition < currentHashMatches.size) ||
          (streamIter.hasNext && fetchNext())
      }

      override final def next() = {
        val ret = buildSide match {
          case BuildRight => joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
          case BuildLeft => joinRow(currentHashMatches(currentMatchPosition), currentStreamedRow)
        }
        currentMatchPosition += 1
        ret
      }

      /**
       * Searches the streamed iterator for the next row that has at least one match in hashtable.
       *
       * @return true if the search is successful, and false if the streamed iterator runs out of
       *         tuples.
       */
      private final def fetchNext(): Boolean = {
        //println("===fetchnext====")
        currentHashMatches = null
        currentMatchPosition = -1


        while (currentHashMatches == null && streamIter.hasNext) {
          currentStreamedRow = streamIter.next()
          if (!joinKeys(currentStreamedRow).anyNull) {
            currentHashMatches = hashedRelation.get(joinKeys.currentValue)
          }
        }

        if (currentHashMatches == null) {
          false
        } else {
          currentMatchPosition = 0
          true
        }
      }
    }

  }
}

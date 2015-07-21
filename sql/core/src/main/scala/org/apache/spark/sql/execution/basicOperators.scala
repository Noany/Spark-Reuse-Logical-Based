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

import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, HashPartitioner, SparkConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, OrderedDistribution, SinglePartition, UnspecifiedDistribution}
import org.apache.spark.util.MutablePair
import org.apache.spark.util.collection.ExternalSorter

//zengdan
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Stats
import scala.collection.mutable.Map

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Project(projectList: Seq[NamedExpression], child: SparkPlan, optionRef: Option[QNodeRef] = None) extends UnaryNode {
  //val newProjectList = projectList.sortWith((x,y) => x.name.compareTo(y.name) < 0)
  override def output = projectList.map(_.toAttribute)


  @transient lazy val buildProjection = newMutableProjection(projectList, child.output)
  @transient lazy val (fixedSize, stringIndexes) = outputSize(output)

  //this.id = optionId
  this.nodeRef = optionRef

  var totalCollect = false

  def execute() = {
    val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
    var newRdd = if(shouldCollect){
        child.execute().mapPartitions { iter =>
          val start = System.nanoTime()
          val resuableProjection = buildProjection()
          time = System.nanoTime() - start

          new Iterator[Row] {
            override final def hasNext = {
              val continue = if(totalCollect) {
                val start = System.nanoTime()
                val x = iter.hasNext
                time += (System.nanoTime() - start)
                x
              }else{
                iter.hasNext
              }
              if (!continue && rowCount != 0) {
                avgSize = (fixedSize + avgSize/rowCount)
                logDebug(s"Project ${nodeRef.get.id}: $time, $rowCount, $avgSize")
                var statistics = Stats.statistics.get()
                if (statistics == null) {
                  //no match in the query graph, so no need to collect information
                  statistics = Map[Int, Array[Int]]()
                  Stats.statistics.set(statistics)
                }
                statistics.put(nodeRef.get.id, Array((time/1e6).toInt, avgSize*rowCount))
              }
              continue
            }

            override final def next() = {
              val result = if(totalCollect){
                val cstart = System.nanoTime()
                val pre = iter.next()
                val x = resuableProjection(pre)
                //time to compute a row from source, not only the operator's processing time
                time += (System.nanoTime() - cstart)
                x
              }else{
                val pre = iter.next()
                val cstart = System.nanoTime()
                val x = resuableProjection(pre)
                //time to compute a row from source, not only the operator's processing time
                time += (System.nanoTime() - cstart)
                x
              }

              rowCount += 1
              for (index <- stringIndexes) {
                //sizeInBytes += result.getString(index).length
                avgSize += result.getString(index).length
              }
              result
            }
          }
        }
    }else{
        child.execute().mapPartitions { iter =>
          val resuableProjection = buildProjection()
          iter.map(resuableProjection)
        }
    }



    if (nodeRef.isDefined && nodeRef.get.cache) {

        newRdd.cacheID = Some(nodeRef.get.id)
        //newRdd = newRdd.sparkContext.saveAndLoadOperatorFile[Row](newRdd.cacheID.get,
        //  newRdd.map(ScalaReflection.convertRowToScala(_, schema)))
        newRdd = SQLContext.cacheData(newRdd, output, nodeRef.get.id)
        //if(!SparkEnv.get.blockManager.tachyonStore.checkGlobalExists(id.get))
        //  rdd.saveAsObjectFile(s"tachyon://localhost:19998/tmp_spark_tachyon/${rdd.cacheID.get}")

        //rdd = rdd.sparkContext.objectFile(s"tachyon://localhost:19998/tmp_spark_tachyon/${rdd.cacheID.get}")
        //rdd.persist(StorageLevel.OFF_HEAP)
        //rdd.clearGlobalDependencies()
    }
    newRdd
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Filter(condition: Expression, child: SparkPlan, optionRef: Option[QNodeRef] = None) extends UnaryNode {
  override def output = child.output

  @transient lazy val (fixedSize, stringIndexes) = outputSize(output)

  @transient lazy val conditionEvaluator = newPredicate(condition, child.output)

  //this.id = optionId
  this.nodeRef = optionRef
  var totalCollect = false  //in case of filter->hivetablescan

  def execute() = {
    val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
    var newRdd = if(shouldCollect){
      child.execute().mapPartitions{ iter =>
        var hd: Row = null
        var hdDefined: Boolean = false
        var flag: Boolean = true
        new Iterator[Row]{

          override def hasNext: Boolean = hdDefined || {
            do {
              val continue = if(totalCollect){
                val start = System.nanoTime()
                val tmp = iter.hasNext
                time += (System.nanoTime() - start)
                tmp
              }else{
                iter.hasNext
              }
              if (!continue) {
                if(rowCount != 0) {
                  avgSize = (fixedSize + avgSize / rowCount)
                  logDebug(s"Filter ${nodeRef.get.id}: $time, $rowCount, $avgSize")
                  var statistics = Stats.statistics.get()
                  if (statistics == null) {
                    statistics = Map[Int, Array[Int]]()
                    Stats.statistics.set(statistics)
                  }
                  statistics.put(nodeRef.get.id, Array((time / 1e6).toInt, avgSize * rowCount))
                }
                return false
              }
              if(totalCollect){
                val start = System.nanoTime()
                hd = iter.next()
                flag = conditionEvaluator(hd)
                time += (System.nanoTime() - start)
              }else {
                hd = iter.next()
                val start = System.nanoTime()
                flag = conditionEvaluator(hd)
                time += (System.nanoTime() - start)
              }
            } while (!flag)
            hdDefined = true
            true
          }

          override def next() = {
            var result: Row = null
            if (hasNext) {
              hdDefined = false
              result = hd
              rowCount += 1
              for (index <- stringIndexes) {
                avgSize += result.getString(index).length
              }
            } else
              throw new NoSuchElementException("next on empty iterator")
            result
          }
        }
      }
    }else {
      child.execute().mapPartitions { iter =>
        iter.filter(conditionEvaluator)
      }
    }
    if (nodeRef.isDefined && nodeRef.get.cache) {
      newRdd.cacheID = Some(nodeRef.get.id)
      newRdd = SQLContext.cacheData(newRdd, output, nodeRef.get.id)
    }
    newRdd
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Sample(fraction: Double, withReplacement: Boolean, seed: Long, child: SparkPlan)
  extends UnaryNode
{
  override def output = child.output

  // TODO: How to pick seed?
  override def execute() = child.execute().sample(withReplacement, fraction, seed)
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Union(children: Seq[SparkPlan]) extends SparkPlan {
  // TODO: attributes output by union should be distinct for nullability purposes
  override def output = children.head.output
  override def execute() = sparkContext.union(children.map(_.execute()))
}

/**
 * :: DeveloperApi ::
 * Take the first limit elements. Note that the implementation is different depending on whether
 * this is a terminal operator or not. If it is terminal and is invoked using executeCollect,
 * this operator uses something similar to Spark's take method on the Spark driver. If it is not
 * terminal or is invoked using execute, we first take the limit on each partition, and then
 * repartition all the data to a single partition to compute the global limit.
 */
@DeveloperApi
case class Limit(limit: Int, child: SparkPlan)
  extends UnaryNode {
  // TODO: Implement a partition local limit, and use a strategy to generate the proper limit plan:
  // partition local limit -> exchange into one partition -> partition local limit again

  /** We must copy rows when sort based shuffle is on */
  private def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  override def output = child.output
  override def outputPartitioning = SinglePartition

  /**
   * A custom implementation modeled after the take function on RDDs but which never runs any job
   * locally.  This is to avoid shipping an entire partition of data in order to retrieve only a few
   * rows.
   */
  override def executeCollect(): Array[Row] = {
    if (limit == 0) {
      return new Array[Row](0)
    }

    val childRDD = child.execute().map(_.copy())

    val buf = new ArrayBuffer[Row]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < limit && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * limit * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = limit - buf.size
      val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)
      val sc = sqlContext.sparkContext
      val res =
        sc.runJob(childRDD, (it: Iterator[Row]) => it.take(left).toArray, p, allowLocal = false)

      res.foreach(buf ++= _.take(limit - buf.size))
      partsScanned += numPartsToTry
    }

    buf.toArray.map(ScalaReflection.convertRowToScala(_, this.schema))
  }

  override def execute() = {
    val rdd: RDD[_ <: Product2[Boolean, Row]] = if (sortBasedShuffleOn) {
      child.execute().mapPartitions { iter =>
        iter.take(limit).map(row => (false, row.copy()))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val mutablePair = new MutablePair[Boolean, Row]()
        iter.take(limit).map(row => mutablePair.update(false, row))
      }
    }
    val part = new HashPartitioner(1)
    val shuffled = new ShuffledRDD[Boolean, Row, Row](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    shuffled.mapPartitions(_.take(limit).map(_._2))
  }
}

/**
 * :: DeveloperApi ::
 * Take the first limit elements as defined by the sortOrder. This is logically equivalent to
 * having a [[Limit]] operator after a [[Sort]] operator. This could have been named TopK, but
 * Spark's top operator does the opposite in ordering so we name it TakeOrdered to avoid confusion.
 */
@DeveloperApi
case class TakeOrdered(limit: Int, sortOrder: Seq[SortOrder], child: SparkPlan) extends UnaryNode {

  override def output = child.output
  override def outputPartitioning = SinglePartition

  val ord = new RowOrdering(sortOrder, child.output)

  // TODO: Is this copying for no reason?
  override def executeCollect() = child.execute().map(_.copy()).takeOrdered(limit)(ord)
    .map(ScalaReflection.convertRowToScala(_, this.schema))

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  override def execute() = sparkContext.makeRDD(executeCollect(), 1)
}

/**
 * :: DeveloperApi ::
 * Performs a sort on-heap.
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 */
@DeveloperApi
case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def execute() = attachTree(this, "sort") {
    child.execute().mapPartitions( { iterator =>
      val ordering = newOrdering(sortOrder, child.output)
      iterator.map(_.copy()).toArray.sorted(ordering).iterator
    }, preservesPartitioning = true)
  }

  override def output = child.output
}

/**
 * :: DeveloperApi ::
 * Performs a sort, spilling to disk as needed.
 * @param global when true performs a global sort of all partitions by shuffling the data first
 *               if necessary.
 */
@DeveloperApi
case class ExternalSort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  override def execute() = attachTree(this, "sort") {
    child.execute().mapPartitions( { iterator =>
      val ordering = newOrdering(sortOrder, child.output)
      val sorter = new ExternalSorter[Row, Null, Row](ordering = Some(ordering))
      sorter.insertAll(iterator.map(r => (r, null)))
      sorter.iterator.map(_._1)
    }, preservesPartitioning = true)
  }

  override def output = child.output
}

/**
 * :: DeveloperApi ::
 * Computes the set of distinct input rows using a HashSet.
 * @param partial when true the distinct operation is performed partially, per partition, without
 *                shuffling the data.
 * @param child the input query plan.
 */
@DeveloperApi
case class Distinct(partial: Boolean, child: SparkPlan, optionRef: Option[QNodeRef] = None) extends UnaryNode {
  override def output = child.output

  this.nodeRef = optionRef

  @transient lazy val (fixedSize, stringIndexes) = if(partial) (0,Nil) else outputSize(output)

  override def requiredChildDistribution =
    if (partial) UnspecifiedDistribution :: Nil else ClusteredDistribution(child.output) :: Nil

  override def execute() = {
    val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
    var newRdd = if(shouldCollect) {
      if (!partial)
        child.nodeRef = nodeRef
      child.execute().mapPartitions { iter =>
        val hashSet = new scala.collection.mutable.HashSet[Row]()

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val start = System.nanoTime()
          if (!hashSet.contains(currentRow)) {
            hashSet.add(currentRow.copy())
            rowCount += 1
            for (index <- stringIndexes) {
              avgSize += currentRow.getString(index).length
            }
          }
          time += (System.nanoTime() - start)
        }


        new Iterator[Row]{
          val newIter = hashSet.iterator
          override def hasNext = {
            val continue = newIter.hasNext
            if (!continue && rowCount != 0) {
              avgSize = (fixedSize + avgSize / rowCount)
              logDebug(s"Distinct: $time, $rowCount, $avgSize")
              var statistics = Stats.statistics.get()
              if (statistics == null) {
                statistics = Map[Int, Array[Int]]()
                Stats.statistics.set(statistics)
              }
              if(partial) {
                statistics.put(nodeRef.get.id, Array((time/1e6).toInt, 0))
              }else
                statistics.put(nodeRef.get.id, Array((time/1e6).toInt, avgSize * rowCount))
            }
            continue
          }
          override def next = newIter.next()
        }
      }
    }else{
      child.execute().mapPartitions { iter =>
        val hashSet = new scala.collection.mutable.HashSet[Row]()

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          if (!hashSet.contains(currentRow)) {
            hashSet.add(currentRow.copy())
          }
        }

        hashSet.iterator
      }
    }

    if(nodeRef.isDefined && nodeRef.get.cache && !partial){
      newRdd.cacheID = Some(nodeRef.get.id)
      //newRdd = newRdd.sparkContext.saveAndLoadOperatorFile[Row](newRdd.cacheID.get, newRdd.map(ScalaReflection.convertRowToScala(_, schema)))
      newRdd = SQLContext.cacheData(newRdd, output, nodeRef.get.id)
    }
    newRdd
  }
}


/**
 * :: DeveloperApi ::
 * Returns a table with the elements from left that are not in right using
 * the built-in spark subtract function.
 */
@DeveloperApi
case class Except(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = left.output

  override def execute() = {
    left.execute().map(_.copy()).subtract(right.execute().map(_.copy()))
  }
}

/**
 * :: DeveloperApi ::
 * Returns the rows in left that also appear in right using the built in spark
 * intersection function.
 */
@DeveloperApi
case class Intersect(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = children.head.output

  override def execute() = {
    left.execute().map(_.copy()).intersection(right.execute().map(_.copy()))
  }
}

/**
 * :: DeveloperApi ::
 * A plan node that does nothing but lie about the output of its child.  Used to spice a
 * (hopefully structurally equivalent) tree from a different optimization sequence into an already
 * resolved tree.
 */
@DeveloperApi
case class OutputFaker(output: Seq[Attribute], child: SparkPlan) extends SparkPlan {
  def children = child :: Nil
  def execute() = child.execute()
}

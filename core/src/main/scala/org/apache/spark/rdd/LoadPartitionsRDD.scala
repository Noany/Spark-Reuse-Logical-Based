package org.apache.spark.rdd

import org.apache.spark.executor.DataReadMethod
import org.apache.spark.{SparkEnv, TaskContext, Partition, SparkContext}
import org.apache.spark.storage.RDDBlockId

import scala.reflect.ClassTag

/**
 * Created by zengdan on 15-3-19.
 */
/*
private[spark]
class LoadPartitionsRDD[U: ClassTag](sc: SparkContext, pars: Array[Partition])
  extends RDD[U](sc, Nil) {

  override def getPartitions: Array[Partition] = pars

  override def compute(split: Partition, context: TaskContext) = {
    val key = RDDBlockId(this.id, split.index, this.cacheID)
    SparkEnv.get.blockManager.tachyonStore.getBytes(key) match {
      case Some(bytes) =>
        Some(new BlockResult(
            dataDeserialize(blockId, bytes), DataReadMethod.Memory, info.size))
      case None =>
        logDebug(s"Block $blockId not found in tachyon")
    }
  }
  }
}
*/

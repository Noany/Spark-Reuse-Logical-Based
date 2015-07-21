package org.apache.spark.sql.auto.cache

/**
 * Created by zengdan on 15-3-13.
 */

import java.util.HashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, Map}

object QueryNode{
  var counter = new AtomicInteger(-1)
}

class QueryNode[T](plan: T) {
  //val parents = new ConcurrentHashMap[Int, ArrayBuffer[QueryNode]]()  //hashcode -> parent
  val parents = new ArrayBuffer[QueryNode[T]]()
  var id: Int = QueryNode.counter.getAndIncrement
  //var cache = false

  val stats: Array[Long] = new Array[Long](3)
  //ref,time, size

  def getPlan: T = plan

  override def toString() = {
    val planClassName = plan.getClass.getName
    val index = planClassName.lastIndexOf(".")

    "id: " + id + " statistics: " + stats(0) + " " + stats(1) + " " + stats(2) +
      " " + planClassName.substring(index+1)
  }

}

class QueryGraph{
  /*
   * TODO: parents synchronized
   */
  val root = new QueryNode[LogicalPlan](null)
  //val parents = new ArrayBuffer[QueryNode[LogicalPlan]]()
  val nodes = new HashMap[Int, QueryNode[LogicalPlan]]() //id -> node

  /*
   * TODO: cut Graph to save space
   */
  def cutGraph(){

  }

  def addNode(curChild:ArrayBuffer[QueryNode[LogicalPlan]],
                               plan: LogicalPlan,
                               refs: HashMap[Int, QNodeRef]): QueryNode[LogicalPlan] = {
    val newNode = new QueryNode(plan)
    curChild.foreach(_.parents.append(newNode))
    nodes.put(newNode.id,newNode)
    plan.nodeRef = Some(QNodeRef(newNode.id, false, true))
    refs.put(plan.id, plan.nodeRef.get)
    newNode.stats(0) = 1
    newNode
  }

  def planRewritten(plan: LogicalPlan):HashMap[Int, QNodeRef] = {
    maxBenefit = Double.MinValue
    maxNode = null
    maxPlan = null
    val refs = new HashMap[Int, QNodeRef]()
    matches(plan, refs)
    //if(maxNode != null){
    //  maxNode.cache = true
    //}
    if(maxPlan != null){
      //maxPlan.cache = true
      maxPlan.nodeRef.get.cache = true
    }
    refs
  }

  var maxBenefit = Double.MinValue
  var maxNode: QueryNode[LogicalPlan] = null
  var maxPlan: LogicalPlan = null

  def update(node: QueryNode[LogicalPlan], plan: LogicalPlan)= {
    //没有统计信息的暂不参与计算
    if(node.stats(2) > 0){
      val benefit = node.stats(0)*node.stats(1)*1.0/node.stats(2)
      if(benefit > maxBenefit){
        maxBenefit = benefit
        maxNode = node
        maxPlan = plan
      }
    }
  }

  //return matchnode, whether match
  def matches(plan: LogicalPlan, refs: HashMap[Int, QNodeRef]):(QueryNode[LogicalPlan], Boolean) = {
    if(plan == null)
      return (null, false)

    if(plan.isInstanceOf[InMemoryRelation]){
      if(!plan.nodeRef.isDefined)
        return (null, false)
      val node = nodes.get(plan.nodeRef.get.id)
      node.stats(0) += 1
      update(node, plan)
      return (node, true)
    }

    if(plan.children == null || plan.children.length <= 0){
      for (leave <- root.parents) {
        if (leave.getPlan.operatorMatch(plan)) {
          leave.stats(0) += 1
          update(leave, plan)
          plan.nodeRef = Some(QNodeRef(leave.id, false, false))
          refs.put(plan.id, plan.nodeRef.get)
          return (leave, true)
        }
      }
      return (addNode(ArrayBuffer(root), plan, refs), false)
    }

    //ensure all children matches
    var i = 0
    val children = new ArrayBuffer[QueryNode[LogicalPlan]]()
    val childrenNode = new ArrayBuffer[QueryNode[LogicalPlan]]()
    while(i < plan.children.length) {
      val (curNode, curMatch) = matches(plan.children(i), refs)
      if (curNode == null)
        return (null, false)
      childrenNode.append(curNode)
      children.append(curNode)
      i += 1
    }

    if(childrenNode.length == plan.children.length) {
      for (candidate <- childrenNode(0).parents) {
        if (candidate.getPlan.operatorMatch(plan)) {
          if((childrenNode.length == 1) ||
            (childrenNode.length > 1 && !childrenNode.exists(!_.parents.contains((candidate))))) {
            candidate.stats(0) += 1
            update(candidate, plan)
            plan.nodeRef = Some(QNodeRef(candidate.id, false, false))
            refs.put(plan.id, plan.nodeRef.get)
            return (candidate, true)
          }
        }
      }
    }
    //add new node
    return (addNode(children, plan, refs), false)
  }

  /*
  def matchesOld(plan: LogicalPlan, refs: HashMap[Int, QNodeRef]):(QueryNode[LogicalPlan], LogicalPlan) = {
    if(plan == null)
      return (null, plan)

    if(plan.isInstanceOf[InMemoryRelation]){
      if(!plan.nodeRef.isDefined)
        return (null, plan)
      val node = nodes.get(plan.nodeRef.get.id)
      node.stats(0) += 1
      //change   not only choose from root
      update(node, plan)
      return (node, plan)
    }

    if(plan.children == null || plan.children.length <= 0){
      for (leave <- parents) {
        if (leave.getPlan.operatorMatch(plan)) {
          leave.stats(0) += 1
          update(leave, plan)
          plan.nodeRef = Some(QNodeRef(leave.id, false, false))
          refs.put(plan.id, plan.nodeRef.get)
          return (leave, plan)
        }
      }

      return (addNodeWithMultiChildren(ArrayBuffer(parents), plan, refs), plan)
    }

    //ensure all children matches
    val (node, newPlan) = matches(plan.children(0), refs)
    if (node == null)
      return (null, newPlan)

    var i = 1
    val nodeParents = new ArrayBuffer[ArrayBuffer[QueryNode[LogicalPlan]]]()
    nodeParents.append(node.parents)
    while(i < plan.children.length) {
      val (curNode, curPlan) = matches(plan.children(i), refs)
      if (curNode == null)
        return (null, curPlan)
      nodeParents.append(curNode.parents)
      i += 1
    }

    for (candidate <- node.parents) {
      if (candidate.getPlan.operatorMatch(plan)) {
        candidate.stats(0) += 1
        update(candidate, plan)
        plan.nodeRef = Some(QNodeRef(candidate.id, false, false))
        refs.put(plan.id, plan.nodeRef.get)
        return (candidate, plan)
      }
    }
    //add new node
    return (addNodeWithMultiChildren(nodeParents, plan, refs), plan)
  }
  */

  def updateStatistics(stats: Map[Int, Array[Long]]) = {
      for((key, value) <- stats){
        val refNode = nodes.get(key)
        if(refNode != null){
          refNode.stats(1) = value(0)  //update time
          refNode.stats(2) = value(1)  //update size
        }
      }
      QueryGraph.printResult(this)
  }
}

object  QueryGraph{
  val qg = new QueryGraph

  def printResult(graph: QueryGraph){
    println("=====Parents=====")
    graph.root.parents.foreach(println)


    println("=====Nodes=====")
    val iter2 = graph.nodes.entrySet().iterator()
    while(iter2.hasNext){
      val cur = iter2.next()
      print(s"${cur.getKey} ")
      print(s"${cur.getValue} ")
      println()
    }

    println("===============")

  }
}

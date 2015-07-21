package org.apache.spark.sql.auto.cache

/**
 * Created by zengdan on 15-3-23.
 */

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util.HashMap
import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.actor.ActorRef
import akka.remote.{DisassociatedEvent, AssociatedEvent, RemotingLifecycleEvent}
import org.apache.spark.SparkEnv._
import org.apache.spark.scheduler.Task
import org.apache.spark._
import org.apache.spark.serializer.{Serializer, SerializerInstance}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.QNodeRef
import org.apache.spark.sql.auto.cache.QGMasterMessages._
import org.apache.spark.storage.BlockManagerMessages.{UpdateBlockInfo, RemoveRdd}
import org.apache.spark.util.{SerializableBuffer, Utils, SignalLogger, AkkaUtils}
import scala.collection.mutable.Map
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.auto.cache.QGUtils.PlanDesc

class QGDriver(sc: SparkContext) extends Actor with Logging{

  //public
  private var qgactor: ActorRef = _
  private val conf = sc.getConf

  override def preStart() = {
    val timeout = AkkaUtils.lookupTimeout(conf)
    qgactor = Await.result(context.actorSelection(QGMaster.toAkkaUrl).resolveOne(timeout), timeout)
    logInfo(s"Actor address in QGDriver is ${qgactor}")
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  override def receive = {
    case MatchSerializedPlan(planDesc) =>
      logInfo("Got serialized plan in QGDriver")
      sender ! rewritePlan(planDesc)

    case UpdateInfo(stats) =>
      sender ! updateStats(stats)

    case RemoveJars(jars) =>
      logInfo("Remove jars in QGDriver")
      sender ! askMasterWithReply[Boolean](RemoveJars(jars))
      //qgactor ! RemoveJars(sc.addedJars)

    case AssociatedEvent(localAddress, remoteAddress, inbound) =>
      logInfo(s"Successfully connected to $remoteAddress")

    case DisassociatedEvent(_, address, _) => {
      //qgactor ! RemoveJars(sc.addedJars)
      logInfo(s"$address got disassociated.")

    }
  }

  override def postStop() {
    qgactor ! RemoveJars(sc.addedJars)
    println("QGDriver in postStop")
  }

  def rewritePlan(planDesc: PlanDesc): HashMap[Int, QNodeRef] = {
    askMasterWithReply[HashMap[Int, QNodeRef]](MatchSerializedPlan(planDesc))
  }

  def updateStats(stats: Map[Int, Array[Long]]) = {
    askMasterWithReply[Boolean](UpdateInfo(stats))
  }

  private def askMasterWithReply[T](message: Any): T = {
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[T](message, qgactor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
  }

}

object QGDriver{
  def createActor(sc: SparkContext): (ActorSystem, ActorRef) = {
    val conf = sc.getConf
    val securityMgr = new SecurityManager(conf)
    val hostname = conf.get("spark.driver.host", Utils.localHostName())
    val (actorSystem, _) = AkkaUtils.createActorSystem("sqlDriver", hostname, 7072, conf,
      securityMgr)
    (actorSystem, actorSystem.actorOf(Props(new QGDriver(sc)), "QGDriver"))
  }

  def rewrittenPlan(plan: LogicalPlan, sqlContext: SQLContext, actor: ActorRef): HashMap[Int, QNodeRef] = {
    //QueryGraph.qg.planRewritten(plan)
    ///*
    val conf = sqlContext.sparkContext.getConf

    val serializer = sqlContext.serializer

    val planBuffer = ByteBuffer.wrap(serializer.newInstance().serialize(plan).array())
    val plandesc = new PlanDesc(sqlContext.sparkContext.addedJars, new SerializableBuffer(planBuffer))

    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout", "60").toLong, "seconds")
    val message = MatchSerializedPlan(plandesc)
    AkkaUtils.askWithReply[HashMap[Int, QNodeRef]](message, actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
    //*/
  }

  def updateStats(stats: Map[Int, Array[Long]], sqlContext: SQLContext, actor: ActorRef): Boolean = {
    val conf = sqlContext.sparkContext.getConf
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[Boolean](UpdateInfo(stats), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
  }

  def removeJars(sqlContext: SQLContext, actor: ActorRef): Boolean = {
    val sc = sqlContext.sparkContext
    val conf = sc.getConf
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[Boolean](RemoveJars(sc.addedJars), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
  }


}

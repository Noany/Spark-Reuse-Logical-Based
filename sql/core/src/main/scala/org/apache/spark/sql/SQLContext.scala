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

package org.apache.spark.sql


import java.lang.String
import java.nio.ByteBuffer
import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.serializer.Serializer
import org.apache.spark.util.{AkkaUtils, Utils, SerializableBuffer}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{AlphaComponent, DeveloperApi, Experimental}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.ExpressionConversions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{Optimizer, DefaultOptimizer}
import org.apache.spark.sql.catalyst.plans.logical.{QNodeRef, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.types.{NativeType, StructType, UserDefinedType}
import org.apache.spark.sql.execution.{SparkStrategies, _}
import org.apache.spark.sql.json._
import org.apache.spark.sql.parquet.ParquetRelation
import org.apache.spark.sql.sources.{DataSourceStrategy, BaseRelation, DDLParser, LogicalRelation}

import org.apache.spark.sql.auto.cache.QGDriver

import scala.collection.mutable.ArrayBuffer

/**
 * :: AlphaComponent ::
 * The entry point for running relational queries using Spark.  Allows the creation of [[SchemaRDD]]
 * objects and the execution of SQL queries.
 *
 * @groupname userf Spark SQL Functions
 * @groupname Ungrouped Support functions for language integrated queries.
 */
@AlphaComponent
class SQLContext(@transient val sparkContext: SparkContext)
  extends org.apache.spark.Logging
  with SQLConf
  with CacheManager
  with ExpressionConversions
  with UDFRegistration
  with Serializable {

  self =>

  //zengdan
  protected[sql]  lazy val (actorSystem, qgDriver) = QGDriver.createActor(sparkContext)

  //zengdan make an id for each logicalplan


  @transient
  protected[sql] lazy val catalog: Catalog = new SimpleCatalog(true)

  @transient
  protected[sql] lazy val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry

  @transient
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, caseSensitive = true)

  @transient
  protected[sql] lazy val optimizer: Optimizer = DefaultOptimizer

  @transient
  protected[sql] val ddlParser = new DDLParser

  @transient
  protected[sql] val sqlParser = {
    val fallback = new catalyst.SqlParser
    new catalyst.SparkSQLParser(fallback(_))
  }

  protected[sql] def parseSql(sql: String): LogicalPlan = {
    ddlParser(sql).getOrElse(sqlParser(sql))
  }

  protected[sql] def executeSql(sql: String): this.QueryExecution = executePlan(parseSql(sql))
  protected[sql] def executePlan(plan: LogicalPlan): this.QueryExecution =
    new this.QueryExecution { val logical = plan }

  sparkContext.getConf.getAll.foreach {
    case (key, value) if key.startsWith("spark.sql") => setConf(key, value)
    case _ =>
  }

  /**
   * :: DeveloperApi ::
   * Allows catalyst LogicalPlans to be executed as a SchemaRDD.  Note that the LogicalPlan
   * interface is considered internal, and thus not guaranteed to be stable.  As a result, using
   * them directly is not recommended.
   */
  @DeveloperApi
  implicit def logicalPlanToSparkQuery(plan: LogicalPlan): SchemaRDD = new SchemaRDD(this, plan)

  /**
   * Creates a SchemaRDD from an RDD of case classes.
   *
   * @group userf
   */
  implicit def createSchemaRDD[A <: Product: TypeTag](rdd: RDD[A]) = {
    SparkPlan.currentContext.set(self)
    val attributeSeq = ScalaReflection.attributesFor[A]
    val schema = StructType.fromAttributes(attributeSeq)
    val rowRDD = RDDConversions.productToRowRdd(rdd, schema)
    new SchemaRDD(this, LogicalRDD(attributeSeq, rowRDD)(self))
  }

  implicit def baseRelationToSchemaRDD(baseRelation: BaseRelation): SchemaRDD = {
    logicalPlanToSparkQuery(LogicalRelation(baseRelation))
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[SchemaRDD]] from an [[RDD]] containing [[Row]]s by applying a schema to this RDD.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val peopleSchemaRDD = sqlContext. applySchema(people, schema)
   *  peopleSchemaRDD.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *    peopleSchemaRDD.registerTempTable("people")
   *  sqlContext.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @group userf
   */
  @DeveloperApi
  def applySchema(rowRDD: RDD[Row], schema: StructType): SchemaRDD = {
    // TODO: use MutableProjection when rowRDD is another SchemaRDD and the applied
    // schema differs from the existing schema on any field data type.
    val logicalPlan = LogicalRDD(schema.toAttributes, rowRDD)(self)
    new SchemaRDD(this, logicalPlan)
  }

  /**
   * Loads a Parquet file, returning the result as a [[SchemaRDD]].
   *
   * @group userf
   */
  def parquetFile(path: String): SchemaRDD =
    new SchemaRDD(this, parquet.ParquetRelation(path, Some(sparkContext.hadoopConfiguration), this))

  /**
   * Loads a JSON file (one object per line), returning the result as a [[SchemaRDD]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group userf
   */
  def jsonFile(path: String): SchemaRDD = jsonFile(path, 1.0)

  /**
   * :: Experimental ::
   * Loads a JSON file (one object per line) and applies the given schema,
   * returning the result as a [[SchemaRDD]].
   *
   * @group userf
   */
  @Experimental
  def jsonFile(path: String, schema: StructType): SchemaRDD = {
    val json = sparkContext.textFile(path)
    jsonRDD(json, schema)
  }

  /**
   * :: Experimental ::
   */
  @Experimental
  def jsonFile(path: String, samplingRatio: Double): SchemaRDD = {
    val json = sparkContext.textFile(path)
    jsonRDD(json, samplingRatio)
  }

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[SchemaRDD]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group userf
   */
  def jsonRDD(json: RDD[String]): SchemaRDD = jsonRDD(json, 1.0)

  /**
   * :: Experimental ::
   * Loads an RDD[String] storing JSON objects (one object per record) and applies the given schema,
   * returning the result as a [[SchemaRDD]].
   *
   * @group userf
   */
  @Experimental
  def jsonRDD(json: RDD[String], schema: StructType): SchemaRDD = {
    val columnNameOfCorruptJsonRecord = columnNameOfCorruptRecord
    val appliedSchema =
      Option(schema).getOrElse(
        JsonRDD.nullTypeToStringType(
          JsonRDD.inferSchema(json, 1.0, columnNameOfCorruptJsonRecord)))
    val rowRDD = JsonRDD.jsonStringToRow(json, appliedSchema, columnNameOfCorruptJsonRecord)
    applySchema(rowRDD, appliedSchema)
  }

  /**
   * :: Experimental ::
   */
  @Experimental
  def jsonRDD(json: RDD[String], samplingRatio: Double): SchemaRDD = {
    val columnNameOfCorruptJsonRecord = columnNameOfCorruptRecord
    val appliedSchema =
      JsonRDD.nullTypeToStringType(
        JsonRDD.inferSchema(json, samplingRatio, columnNameOfCorruptJsonRecord))
    val rowRDD = JsonRDD.jsonStringToRow(json, appliedSchema, columnNameOfCorruptJsonRecord)
    applySchema(rowRDD, appliedSchema)
  }

  /**
   * :: Experimental ::
   * Creates an empty parquet file with the schema of class `A`, which can be registered as a table.
   * This registered table can be used as the target of future `insertInto` operations.
   *
   * {{{
   *   val sqlContext = new SQLContext(...)
   *   import sqlContext._
   *
   *   case class Person(name: String, age: Int)
   *   createParquetFile[Person]("path/to/file.parquet").registerTempTable("people")
   *   sql("INSERT INTO people SELECT 'michael', 29")
   * }}}
   *
   * @tparam A A case class type that describes the desired schema of the parquet file to be
   *           created.
   * @param path The path where the directory containing parquet metadata should be created.
   *             Data inserted into this table will also be stored at this location.
   * @param allowExisting When false, an exception will be thrown if this directory already exists.
   * @param conf A Hadoop configuration object that can be used to specify options to the parquet
   *             output format.
   *
   * @group userf
   */
  @Experimental
  def createParquetFile[A <: Product : TypeTag](
      path: String,
      allowExisting: Boolean = true,
      conf: Configuration = new Configuration()): SchemaRDD = {
    new SchemaRDD(
      this,
      ParquetRelation.createEmpty(
        path, ScalaReflection.attributesFor[A], allowExisting, conf, this))
  }

  /**
   * Registers the given RDD as a temporary table in the catalog.  Temporary tables exist only
   * during the lifetime of this instance of SQLContext.
   *
   * @group userf
   */
  def registerRDDAsTable(rdd: SchemaRDD, tableName: String): Unit = {
    catalog.registerTable(None, tableName, rdd.queryExecution.logical)
  }

  /**
   * Drops the temporary table with the given table name in the catalog. If the table has been
   * cached/persisted before, it's also unpersisted.
   *
   * @param tableName the name of the table to be unregistered.
   *
   * @group userf
   */
  def dropTempTable(tableName: String): Unit = {
    tryUncacheQuery(table(tableName))
    catalog.unregisterTable(None, tableName)
  }

  /**
   * Executes a SQL query using Spark, returning the result as a SchemaRDD.  The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group userf
   */
  def sql(sqlText: String): SchemaRDD = {
    if (dialect == "sql") {
      new SchemaRDD(this, parseSql(sqlText))
    } else {
      sys.error(s"Unsupported SQL dialect: $dialect")
    }
  }

  /** Returns the specified table as a SchemaRDD */
  def table(tableName: String): SchemaRDD =
    new SchemaRDD(this, catalog.lookupRelation(None, tableName))

  /**
   * :: DeveloperApi ::
   * Allows extra strategies to be injected into the query planner at runtime.  Note this API
   * should be consider experimental and is not intended to be stable across releases.
   */
  @DeveloperApi
  var extraStrategies: Seq[Strategy] = Nil

  protected[sql] class SparkPlanner extends SparkStrategies {
    val sparkContext: SparkContext = self.sparkContext

    val sqlContext: SQLContext = self

    def codegenEnabled = self.codegenEnabled

    def numPartitions = self.numShufflePartitions

    val strategies: Seq[Strategy] =
      extraStrategies ++ (
      CommandStrategy(self) ::
      DataSourceStrategy ::
      TakeOrdered ::
      HashAggregation ::
      LeftSemiJoin ::
      HashJoin ::
      InMemoryScans ::
      ParquetOperations ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil)

    /**
     * Used to build table scan operators where complex projection and filtering are done using
     * separate physical operators.  This function returns the given scan operator with Project and
     * Filter nodes added only when needed.  For example, a Project operator is only used when the
     * final desired output requires complex expressions to be evaluated or when columns can be
     * further eliminated out after filtering has been done.
     *
     * The `prunePushedDownFilters` parameter is used to remove those filters that can be optimized
     * away by the filter pushdown optimization.
     *
     * The required attributes for both filtering and expression evaluation are passed to the
     * provided `scanBuilder` function so that it can avoid unnecessary column materialization.
     */
    def pruneFilterProject(
        projectList: Seq[NamedExpression],
        filterPredicates: Seq[Expression],
        prunePushedDownFilters: Seq[Expression] => Seq[Expression],
        scanBuilder: Seq[Attribute] => SparkPlan,
        optionRef: Option[QNodeRef] = None): SparkPlan = {

      val projectSet = AttributeSet(projectList.flatMap(_.references))
      val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
      val filterCondition = prunePushedDownFilters(filterPredicates).reduceLeftOption(And)

      // Right now we still use a projection even if the only evaluation is applying an alias
      // to a column.  Since this is a no-op, it could be avoided. However, using this
      // optimization with the current implementation would change the output schema.
      // TODO: Decouple final output schema from expression evaluation so this copy can be
      // avoided safely.

      if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
          filterSet.subsetOf(projectSet)) {
        // When it is possible to just use column pruning to get the right projection and
        // when the columns of this projection are enough to evaluate all filter conditions,
        // just do a scan followed by a filter, with no extra project.
        val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
        //zengdan
        if(!filterCondition.isDefined) {
          scan.nodeRef = optionRef
          scan
        }
        else{
          val filter = Filter(filterCondition.get, scan, optionRef)
          filter.totalCollect = true
          filter
        }
        //filterCondition.map(Filter(_, scan, optionRef)).getOrElse(scan)
      } else {
        val scan = scanBuilder((projectSet ++ filterSet).toSeq)
        val project = Project(projectList, filterCondition.map(Filter(_, scan)).getOrElse(scan), optionRef)
        project.totalCollect = true
        project
      }
    }
  }

  @transient
  protected[sql] val planner = new SparkPlanner

  @transient
  protected[sql] lazy val emptyResult = sparkContext.parallelize(Seq.empty[Row], 1)

  /**
   * Prepares a planned SparkPlan for execution by inserting shuffle operations as needed.
   */
  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches =
      Batch("Add exchange", Once, AddExchange(self)) :: Nil
  }

  //zengdan
  lazy val serializer = {
    val conf = sparkContext.getConf
    val className = conf.get("spark.sql.plan.serializer", "org.apache.spark.serializer.JavaSerializer")
    val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
    try {
      cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[Serializer]
    } catch {
      case _: NoSuchMethodException =>
        cls.getConstructor().newInstance().asInstanceOf[Serializer]
    }
  }



  var planId = 0

  def getOptimizedPlan(plan: LogicalPlan): LogicalPlan = {
    if(this.sparkContext.getConf.get("spark.sql.auto.cache", "false").toBoolean){
      planId = makeIdForLogicalPlan(plan, planId)
      //recoveryContext(plan, QGDriver.rewrittenPlan(plan, this, qgDriver))
      setNodeRef(plan, QGDriver.rewrittenPlan(plan, this, qgDriver))
    }
    plan
  }


  def makeIdForLogicalPlan(plan: LogicalPlan, startId: Int): Int = {
    if(plan != null){
      var id = startId
      for(child <- plan.children){
        id = makeIdForLogicalPlan(child, id)
      }
      plan.id = id
      id+1
    }else {
      startId
    }
  }

  def setNodeRef(plan: LogicalPlan, refs: HashMap[Int, QNodeRef]){
    if(plan != null){
      if(refs.get(plan.id) != null){
        plan.nodeRef = Some(refs.get(plan.id))
      }
      for(child <- plan.children){
        setNodeRef(child, refs)
      }
    }
  }

  def recoveryContext(plan1: LogicalPlan, plan2: LogicalPlan): LogicalPlan = {
    val rdds1 = getTableRDD(plan1)
    val rdds2 = getTableRDD(plan2)
    for((id,lrdd) <- rdds2){
      val lr = rdds1.get(id)
      if(lr.isDefined){
        lrdd.rdd.setContextAndDep(lr.get.rdd)
      }
    }
    plan2
  }

  private def getTableRDD(plan: LogicalPlan): Map[Int,LogicalRDD] = {
    var rdds = Map[Int,LogicalRDD]()
    if(plan.isInstanceOf[LogicalRDD]){
      val lr = plan.asInstanceOf[LogicalRDD]
      rdds += (lr.rdd.id->lr)
    }else{
      for(child <- plan.children){
        rdds ++= getTableRDD(child)
      }
    }
    rdds
  }

  def stop() = {
    if(this.sparkContext.getConf.get("spark.sql.auto.cache", "false").toBoolean) {
      QGDriver.removeJars(this, qgDriver)
      //actorSystem.shutdown()
    }
  }



  /**
   * :: DeveloperApi ::
   * The primary workflow for executing relational queries using Spark.  Designed to allow easy
   * access to the intermediate phases of query execution for developers.
   */
  @DeveloperApi
  protected abstract class QueryExecution {
    def logical: LogicalPlan

    lazy val analyzed = ExtractPythonUdfs(analyzer(logical))
    lazy val withCachedData = useCachedData(analyzed)
    lazy val optimizedPlanOriginal = optimizer(withCachedData)
    lazy val optimizedPlan = getOptimizedPlan(optimizedPlanOriginal)

    // TODO: Don't just pick the first one...
    lazy val sparkPlan = {
      SparkPlan.currentContext.set(self)    //如何利用
      planner(optimizedPlan).next()
    }
    // executedPlan should not be used to initialize any SparkPlan. It should be
    // only used for execution.
    lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[Row] = executedPlan.execute()
    //lazy val toRdd: RDD[Row] = executedPlan.executeAndCount(times)

    protected def stringOrError[A](f: => A): String =
      try f.toString catch { case e: Throwable => e.toString }

    def simpleString: String =
      s"""== Physical Plan ==
         |${stringOrError(executedPlan)}
      """.stripMargin.trim

    override def toString: String =
      // TODO previously will output RDD details by run (${stringOrError(toRdd.toDebugString)})
      // however, the `toRdd` will cause the real execution, which is not what we want.
      // We need to think about how to avoid the side effect.
      s"""== Parsed Logical Plan ==
         |${stringOrError(logical)}
         |== Analyzed Logical Plan ==
         |${stringOrError(analyzed)}
         |== Optimized Logical Plan ==
         |${stringOrError(optimizedPlan)}
         |== Physical Plan ==
         |${stringOrError(executedPlan)}
         |Code Generation: ${stringOrError(executedPlan.codegenEnabled)}
         |== RDD ==
      """.stripMargin.trim
  }

  /**
   * Parses the data type in our internal string representation. The data type string should
   * have the same format as the one generated by `toString` in scala.
   * It is only used by PySpark.
   */
  private[sql] def parseDataType(dataTypeString: String): DataType = {
    DataType.fromJson(dataTypeString)
  }

  /**
   * Apply a schema defined by the schemaString to an RDD. It is only used by PySpark.
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schemaString: String): SchemaRDD = {
    val schema = parseDataType(schemaString).asInstanceOf[StructType]
    applySchemaToPythonRDD(rdd, schema)
  }

  /**
   * Apply a schema defined by the schema to an RDD. It is only used by PySpark.
   */
  private[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schema: StructType): SchemaRDD = {

    def needsConversion(dataType: DataType): Boolean = dataType match {
      case ByteType => true
      case ShortType => true
      case FloatType => true
      case DateType => true
      case TimestampType => true
      case ArrayType(_, _) => true
      case MapType(_, _, _) => true
      case StructType(_) => true
      case udt: UserDefinedType[_] => needsConversion(udt.sqlType)
      case other => false
    }

    val convertedRdd = if (schema.fields.exists(f => needsConversion(f.dataType))) {
      rdd.map(m => m.zip(schema.fields).map {
        case (value, field) => EvaluatePython.fromJava(value, field.dataType)
      })
    } else {
      rdd
    }

    val rowRdd = convertedRdd.mapPartitions { iter =>
      iter.map { m => new GenericRow(m): Row}
    }

    new SchemaRDD(this, LogicalRDD(schema.toAttributes, rowRdd)(self))
  }
}

object SQLContext{
  def cacheData(rdd: RDD[Row], output: Seq[Attribute], id: Int) = {
    val schemaRow = new GenericRow(output.map(_.name).toArray[Any])
    val schemaRdd:RDD[Row] = rdd.sparkContext.parallelize(Array(schemaRow))

    //save data and schema
    val (data:RDD[Row], schema:RDD[Row], reread:Boolean) =
      rdd.sparkContext.saveAndLoadOperatorFile[Row](id,
        rdd.map(ScalaReflection.convertRowToScala(_, StructType.fromAttributes(output))), schemaRdd)

    //read schema
    if (reread) {
      val loadSchemaRdd = schema.collect()
      require(loadSchemaRdd.size == 1, "Length of schema is not 1!")
      require(loadSchemaRdd(0).size == schemaRow.size, "Length of schema doesn't match!")

      val schemaWithIndex = scala.collection.mutable.Map[String, Int]()
      schemaWithIndex ++= output.map(_.name).zipWithIndex

      val schemaIndexMap = scala.collection.mutable.Map[Int, Int]()
      var i = 0
      while (i < loadSchemaRdd(0).size) {
        //input -> output
        schemaIndexMap += (i -> schemaWithIndex(loadSchemaRdd(0).getString(i)))
        i += 1
      }
      data.mapPartitions { loadIter =>
        new Iterator[Row] {
          private val mutableRow = new GenericMutableRow(schemaRow.size)

          override def hasNext = loadIter.hasNext

          override def next = {
            val input = loadIter.next()
            var i = 0
            while (i < schemaRow.size) {
              val outIndex = schemaIndexMap(i)
              mutableRow(outIndex) = output(outIndex).dataType match{
                case IntegerType => input.getInt(i)
                case BooleanType => input.getBoolean(i)
                case LongType =>    input.getLong(i)
                case DoubleType =>  input.getDouble(i)
                case FloatType =>   input.getFloat(i)
                case ShortType =>   input.getShort(i)
                case ByteType =>    input.getByte(i)
                case StringType =>  input.getString(i)
              }
              i += 1
            }
            mutableRow
          }
        }
      }
    }else{
      data
    }
  }
}

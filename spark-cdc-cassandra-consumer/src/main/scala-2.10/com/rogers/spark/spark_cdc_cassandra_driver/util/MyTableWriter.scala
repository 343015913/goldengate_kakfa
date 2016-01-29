
package com.rogers.spark.spark_cdc_cassandra_driver.util

import java.io.IOException

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.util.Quote._
import org.apache.spark.{ Logging, TaskContext }
import com.datastax.spark.connector.writer._

import org.apache.spark.metrics._
import org.apache.spark.streaming.dstream._
import org.apache.spark.SparkContext

import scala.reflect.runtime.universe._

/**
 * This is a temp hack to allow writing sparse row into Cassandra - Real fix has been made in core Cassandra (https://datastax-oss.atlassian.net/browse/JAVA-777), and  Spark Connector release (https://datastax-oss.atlassian.net/browse/SPARKC-283). Will be avalible in DSE 5.0... for now:
 *
 * The problem is that PreparedStatment inserts tombstones when a value of a column is bound to NULL - saveToCassandra creates a prepared statement for each RDD, with columnNames as a list of expected columns (all columns of a table in our case)
 * Update Mutation usually have only a few columns, resulting in the missing columns being set to null and then deleted.
 *
 * Here we use RegularStatements instead of PreparedStatements to circumvent the problem  - however, this hurts performance as (1) PreparedStatements are better, (2) we don't bother batching them
 */

/**
 * Stolen from Spark Connector 1.5
 * An option with an extra bit of information to let us know whether a value should be treated
 * as a delete or as an unsetIfNone value. Reading a table using this as an column type will
 * cause all empty values to not be treated as deletes on write.
 */

object Unset extends Serializable //an object representing a column which will be skipped on insert.
case class CassandraOption[A](option: Option[A], unsetIfNone: Boolean = true)

object CassandraOption {
  def Unset[A]: CassandraOption[A] = CassandraOption[A](None, true)
  def Delete[A]: CassandraOption[A] = CassandraOption[A](None, false)
}
class OptionToNullConverter(nestedConverter: TypeConverter[_]) {
  def targetTypeTag = implicitly[TypeTag[AnyRef]]

  /** String representation of the converter target type.*/
  def targetTypeName: String = TypeTag.synchronized(
    targetTypeTag.tpe.toString)

  def cassandraOptionToAnyRef(cassandraOption: CassandraOption[_]) = {
    cassandraOption.option match {
      case Some(x) => nestedConverter.convert(x).asInstanceOf[AnyRef]
      case None    => if (cassandraOption.unsetIfNone == false) null else Unset
    }
  }

  def convertPF: PartialFunction[Any, AnyRef] = {
    case x: CassandraOption[_] => cassandraOptionToAnyRef(x)
    case Some(x)               => nestedConverter.convert(x).asInstanceOf[AnyRef]
    case None                  => null
    case Unset                 => Unset
    case x                     => nestedConverter.convert(x).asInstanceOf[AnyRef]
  }
  def convert(obj: Any): AnyRef = {
    convertPF.applyOrElse(obj, (_: Any) =>
      if (obj != null)
        throw new Exception(s"Cannot convert object $obj of type ${obj.getClass} to $targetTypeName.")
      else
        throw new Exception(s"Cannot convert object $obj to $targetTypeName."))
  }
}

class DStreamFunctions[T](dstream: DStream[T]) extends Serializable {

  def sparkContext: SparkContext = dstream.context.sparkContext

  def conf = sparkContext.getConf

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */

  //TODO: RowWriterFactory is pretty useless here - it was copied from the Spark Connector, and is used to create RowWriters that provides getters for Column names and Values from T
  // We do it simpler here and never use RowWriterFactory. Thing is - if we remove it, MyTableWriter cannot determine T....
  // Implictis in Scala are waaaay to complicated
  def saveToCassandraSimple(
    keyspaceName: String,
    tableName: String,
    columnNames: ColumnSelector = AllColumns,
    writeConf: WriteConf = WriteConf.fromSparkConf(conf))(
      implicit connector: CassandraConnector = CassandraConnector(conf),
      rwf: RowWriterFactory[T]): Unit = {

    val writer = MyTableWriter(connector, keyspaceName, tableName, columnNames, writeConf)
    val temp = writer.write _
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, temp))
  }
}

class MyTableWriter[T](
    connector: CassandraConnector,
    tableDef: TableDef,
    columnSelector: IndexedSeq[ColumnRef],
    rowWriter: RowWriter[T],
    writeConf: WriteConf) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName

  //val columnNames = rowWriter.columnNames diff writeConf.optionPlaceholders - TODO: we're ignoring options
  val columnNames = rowWriter.columnNames
  val columns = columnNames.map(tableDef.columnByName)

  var rowIterator: CountingIterator[T] = _

  //def cassandraOptionConverter = new OptionToNullConverter()

  implicit val protocolVersion = connector.withClusterDo { _.getConfiguration.getProtocolOptions.getProtocolVersionEnum }

  val defaultTTL = writeConf.ttl match {
    case TTLOption(StaticWriteOptionValue(value)) => Some(value)
    case _                                        => None
  }

  val defaultTimestamp = writeConf.timestamp match {
    case TimestampOption(StaticWriteOptionValue(value)) => Some(value)
    case _ => None
  }
  /* private def columnNames(row: CassandraRow): Seq[String] = {
    row.columnNames
  }
  private def columns(row: CassandraRow) = columnNames(row).map(tableDef.columnByName)
  *
  */
  private def queryTemplateUsingInsert(columnNames: Seq[String]): String = {
    val quotedColumnNames: Seq[String] = columnNames.map(quote)
    val columnSpec = quotedColumnNames.mkString(", ")
    val valueSpec = quotedColumnNames.map(":" + _).mkString(", ")
    val ttlSpec = writeConf.ttl match {
      case TTLOption(PerRowWriteOptionValue(placeholder)) => Some(s"TTL :$placeholder")
      case TTLOption(StaticWriteOptionValue(value)) => Some(s"TTL $value")
      case _ => None
    }

    val timestampSpec = writeConf.timestamp match {
      case TimestampOption(PerRowWriteOptionValue(placeholder)) => Some(s"TIMESTAMP :$placeholder")
      case TimestampOption(StaticWriteOptionValue(value)) => Some(s"TIMESTAMP $value")
      case _ => None
    }

    val options = List(ttlSpec, timestampSpec).flatten
    val optionsSpec = if (options.nonEmpty) s"USING ${options.mkString(" AND ")}" else ""

    s"INSERT INTO ${quote(keyspaceName)}.${quote(tableName)} ($columnSpec) VALUES ($valueSpec) $optionsSpec".trim
  }

  private def queryTemplateUsingUpdate(columnNames: Seq[String]): String = {
    val (primaryKey, regularColumns) = columns.partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    val nameToBehavior = (columnSelector collect {
      case cn: CollectionColumnName => cn.columnName -> cn.collectionBehavior
    }).toMap

    val setNonCounterColumnsClause = for {
      colDef <- nonCounterColumns
      name = colDef.columnName
      collectionBehavior = nameToBehavior.get(name)
      quotedName = quote(name)
    } yield collectionBehavior match {
      case Some(CollectionAppend)           => s"$quotedName = $quotedName + :$quotedName"
      case Some(CollectionPrepend)          => s"$quotedName = :$quotedName + $quotedName"
      case Some(CollectionRemove)           => s"$quotedName = $quotedName - :$quotedName"
      case Some(CollectionOverwrite) | None => s"$quotedName = :$quotedName"
    }

    def quotedColumnNames(columns: Seq[ColumnDef]) = columns.map(_.columnName).map(quote)
    val setCounterColumnsClause = quotedColumnNames(counterColumns).map(c => s"$c = $c + :$c")
    val setClause = (setNonCounterColumnsClause ++ setCounterColumnsClause).mkString(", ")
    val whereClause = quotedColumnNames(primaryKey).map(c => s"$c = :$c").mkString(" AND ")

    s"UPDATE ${quote(keyspaceName)}.${quote(tableName)} SET $setClause WHERE $whereClause"
  }
  private val isCounterUpdate =
    tableDef.columns.exists(_.isCounterColumn)

  private val containsCollectionBehaviors =
    columnSelector.exists(_.isInstanceOf[CollectionColumnName])
  private def queryTemplate(columnNames: Seq[String]): String = {
    if (isCounterUpdate || containsCollectionBehaviors)
      queryTemplateUsingUpdate(columnNames)
    else
      queryTemplateUsingInsert(columnNames)
  }
  private def createStatement(row: T): Statement = {
    var CRow = row.asInstanceOf[CassandraRow]
    println("createStatement")
    println(CRow)
    println(CRow.columnValues)

    //val columnTypes = columnNames(row.asInstanceOf[CassandraRow]).map(preparedStmt.getVariables.getType)//TODO
    val columnTypes = columnNames.map(tableDef.columnByName(_).columnType)
    //val converters = columnTypes.map(_.converterToCassandra)
    val converters = columnTypes.map { t => new OptionToNullConverter(t.converterToCassandra) }
    //val vals = CRow.columnValues.map(cassandraOptionConverter.convert)
    // println(vals)
    //println(CRow.columnValues.zipWithIndex.map { case (c, i) => converters(i).convert(c) })
    //println(CRow.get[Int]("objid"))
    //new SimpleStatement(queryTemplate(columnNames), CRow.columnValues.zipWithIndex.map { case (c, i) => converters(i).convert(c) }: _*) // TODO: Fix this nonsense
    /* Not functional
    val curColumnNames: Seq[String] = scala.collection.mutable.ArrayBuffer.empty
    val curColumnVals: Seq[_] = scala.collection.mutable.ArrayBuffer.empty
    for (i <- 0 until columnNames.size) {
      val converter = converters(i)
      val columnName = columnNames(i)
      val columnValue = converter.convert(CRow.columnValues(i))

      if (columnValue != Unset) {
        curColumnNames :+ columnName
        curColumnVals :+ columnValue
      }
    }
    *
    */
    /* More functional
     * val keyVals = (0 until columnNames.size).flatMap { i =>
      val converter = converters(i)
      val columnName = columnNames(i)
      val columnValue = converter.convert(CRow.columnValues(i))
      columnValue match {
        case Unset => None
        case _     => Some(columnName -> columnValue)
      }
          val (curColumnNames, curColumnVals) = keyVals.unzip
    */
    //Functional :)
    // The logic here is - If the converted value is Unset, we ignore it (basically just updating the other fields)
    // Otherwise we pass the value  to the statement - either updating the value or deleting it if the val is null.
    val keyVals = for (
      i <- 0 until columnNames.size;
      converter = converters(i);
      columnValue = converter.convert(CRow.columnValues(i));
      if (columnValue != Unset)
    ) yield { (columnNames(i), columnValue) }
    val (curColumnNames, curColumnVals) = keyVals.unzip
    new SimpleStatement(queryTemplate(curColumnNames), curColumnVals) // TODO: Fix this nonsense

  }

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]) {
    val updater = OutputMetricsUpdater(taskContext, writeConf)
    connector.withSessionDo { session =>
      rowIterator = new CountingIterator(data)
      //val stmt = prepareStatement(session).setConsistencyLevel(writeConf.consistencyLevel)
      val queryExecutor = new AsyncExecutor[Statement, ResultSet](
        stmt => session.executeAsync(stmt),
        writeConf.parallelismLevel,
        None, None)
      //new QueryExecutor(session, writeConf.parallelismLevel,
      //Some(updater.batchFinished(success = true, _, _, _)), Some(updater.batchFinished(success = false, _, _, _)))

      val rateLimiter = new RateLimiter((writeConf.throughputMiBPS * 1024 * 1024).toLong, 1024 * 1024)

      logDebug(s"Writing data partition to $keyspaceName.$tableName in batches of ${writeConf.batchSize}.")

      for (row <- data) {
        queryExecutor.executeAsync(createStatement(row))
        //assert(stmtToWrite.bytesCount > 0)
        // rateLimiter.maybeSleep(stmtToWrite.bytesCount) // TODO
      }

      queryExecutor.waitForCurrentlyExecutingTasks()

      if (!queryExecutor.successful)
        throw new IOException(s"Failed to write statements to $keyspaceName.$tableName.")

      val duration = updater.finish() / 1000000000d
      logInfo(f"Wrote ${rowIterator.count} rows to $keyspaceName.$tableName in $duration%.3f s.")
    }
  }
}
object MyTableWriter {
  def apply[T: RowWriterFactory](

    connector: CassandraConnector,
    keyspaceName: String,
    tableName: String,
    columnNames: ColumnSelector,
    writeConf: WriteConf): MyTableWriter[T] = {

    val schema = com.datastax.spark.connector.cql.Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = columnNames.selectFrom(tableDef)
    //TODO: We're ignoring optionColumns (TTL and Timestamp)
    // val optionColumns = writeConf.optionsAsColumns(keyspaceName, tableName)
    //val optionColumns = writeConf.optionsAsColumns(keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef.copy(regularColumns = tableDef.regularColumns),
      selectedColumns)

    new MyTableWriter(connector, tableDef, selectedColumns, rowWriter, writeConf)
  }
}
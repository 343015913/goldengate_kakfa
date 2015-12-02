 
package com.rogers.spark.spark_cdc_cassandra_driver.util

import java.io.IOException

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.util.Quote._
import org.apache.spark.{Logging, TaskContext}
import com.datastax.spark.connector.writer._

import org.apache.spark.metrics._
import org.apache.spark.streaming.dstream._
import org.apache.spark.SparkContext


 /**
   * This is a temp hack to allow writing sparse row into Cassandra - Real fix has been made in core Cassandra (https://datastax-oss.atlassian.net/browse/JAVA-777), and will eventually make it's way to the DataStax release (needs support in Cassandra Core, Cassandra Driver, Cassandra Spark Driver)
   * 
   * The problem is that PreparedStatment inserts tombstones when a value of a column is bound to NULL - saveToCassandra creates a prepared statement for each RDD, with columnNames as a list of expected columns (all columns of a table in our case)
   * Update Mutation usually have only a few columns, resulting in the missing columns being set to null and then deleted. 
   * 
   * Here we use RegularStatements instead of PreparedStatements to circumvent the problem  - however, this hurts performance as (1) PreparedStatements are better, (2) we don't bother batching them
   */


class DStreamFunctions[T](dstream: DStream[T]) extends Serializable {

   def sparkContext: SparkContext = dstream.context.sparkContext
 
  def conf = sparkContext.getConf

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */
  def saveToCassandraSimple(
    keyspaceName: String,
    tableName: String,
    columnNames: ColumnSelector = AllColumns,
    writeConf: WriteConf = WriteConf.fromSparkConf(conf))(
  implicit
    connector: CassandraConnector = CassandraConnector(conf),
    rwf: RowWriterFactory[T]): Unit = {

    val writer = MyTableWriter(connector, keyspaceName, tableName, columnNames, writeConf)
    val temp = writer.write _
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, temp))
  }
}

class MyTableWriter[T]  (
    connector: CassandraConnector,
    tableDef: TableDef,
    columnSelector: IndexedSeq[ColumnRef],
 
    writeConf: WriteConf) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  var rowIterator: CountingIterator[T] =_

 
  implicit val protocolVersion = connector.withClusterDo { _.getConfiguration.getProtocolOptions.getProtocolVersionEnum }

  val defaultTTL = writeConf.ttl match {
    case TTLOption(StaticWriteOptionValue(value)) => Some(value)
    case _ => None
  }

  val defaultTimestamp = writeConf.timestamp match {
    case TimestampOption(StaticWriteOptionValue(value)) => Some(value)
    case _ => None
  }
  private def columnNames(row:CassandraRow): Seq[String] = {
      row.columnNames
  }
  private def columns(row:CassandraRow) = columnNames(row).map(tableDef.columnByName)
  private def queryTemplateUsingInsert(row:CassandraRow): String = {
    val quotedColumnNames: Seq[String] = columnNames(row).map(quote)
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

  private def queryTemplateUsingUpdate(row:CassandraRow): String = {
    val (primaryKey, regularColumns) = columns(row).partition(_.isPrimaryKeyColumn)
    val (counterColumns, nonCounterColumns) = regularColumns.partition(_.isCounterColumn)

    val nameToBehavior = (columnSelector collect {
        case cn:CollectionColumnName => cn.columnName -> cn.collectionBehavior
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
   private def queryTemplate(row:CassandraRow): String = {
    if (isCounterUpdate || containsCollectionBehaviors)
      queryTemplateUsingUpdate(row)
    else
      queryTemplateUsingInsert(row)
  }
  private def createStatement(row:T) : Statement = {
    var CRow = row.asInstanceOf[CassandraRow]
    println ("createStatement")
    println (CRow)
    println(CRow.columnValues)
   
    //val columnTypes = columnNames(row.asInstanceOf[CassandraRow]).map(preparedStmt.getVariables.getType)//TODO
    val columnTypes = columnNames(CRow).map(tableDef.columnByName(_).columnType)
    val converters = columnTypes.map(_.converterToCassandra)
     println(CRow.columnValues.zipWithIndex.map{ case(c,i) => converters(i).convert(c)})
     println(CRow.get[Int]("objid"))
     new SimpleStatement(queryTemplate(CRow), CRow.columnValues.zipWithIndex.map{ case(c,i) => converters(i).convert(c)}:_*) // TODO: Fix this nonsense

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
 def apply[T : RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      columnNames: ColumnSelector,
      writeConf: WriteConf): MyTableWriter[T] = {

    val schema = com.datastax.spark.connector.cql.Schema.fromCassandra(connector, Some(keyspaceName), Some(tableName))
    val tableDef = schema.tables.headOption
      .getOrElse(throw new IOException(s"Table not found: $keyspaceName.$tableName"))
    val selectedColumns = columnNames.selectFrom(tableDef)
   // val optionColumns = writeConf.optionsAsColumns(keyspaceName, tableName)

    new MyTableWriter(connector, tableDef, selectedColumns, writeConf)
  }
}
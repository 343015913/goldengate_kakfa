//
// to stream data into the cluster open up netcat and echo sample GGRowRecrods to it, one per line.
//
// nc -lk 9999
// 2014-10-07T12:20:09Z;foo;1
// 2014-10-07T12:21:09Z;foo;29
// 2014-10-07T12:22:10Z;foo;1
// 2014-10-07T12:23:11Z;foo;29
package ute_test
//import scala.language.dynamics

//import com.goldengate.delivery.handler.kafka.util.Encryptor





import kafka.serializer.StringDecoder


import kafka.serializer.DefaultDecoder
import java.net._
import java.io._
import scala.io._
import java.util.Properties
import java.util.Date
import java.util.Random
import java.util.TimeZone
import kafka.producer.{ProducerConfig, KeyedMessage, Producer}
import com.github.nscala_time.time.Imports._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.mapper._
import UTE.util._
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArraySeq
import com.datastax.spark.connector._
import org.apache.spark.storage.StorageLevel
import com.rogers.goldengate.kafka.KafkaUtil

import scala.collection.JavaConverters._



import org.apache.commons.codec.binary.Hex

//import com.rogers.goldengate.serializers.AvroDeserializer

import com.rogers.goldengate.kafka.serializers.KafkaAvroMutationDecoder
import com.rogers.goldengate.api.mutations._
import java.io.Serializable

import com.datastax.spark.connector.types.{MapType, ListType, ColumnType}
import org.apache.spark.metrics.OutputMetricsUpdater

import com.datastax.driver.core.BatchStatement.Type
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.CountingIterator
import com.datastax.spark.connector.util.Quote._
import org.apache.spark.{Logging, TaskContext}
import com.datastax.spark.connector.writer._

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag





//TODO: Somehow need to extned/proxy/wrap CassandraRow properly - this is a hack
case class UniqueCassandraRow(val row: CassandraRow) extends Serializable{
     def getRow = row
} 



/** A [[RowWriter]] that can write SparkSQL `Row` objects. */
/*class UniqueRowWriter(val table: TableDef, val selectedColumns: IndexedSeq[ColumnRef])
  extends RowWriter[UniqueCassandraRow] {

//  override val columnNames = selectedColumns.map(_.columnName)

  //private val columns = columnNames.map(table.columnByName)
  //private val converters = columns.map(_.columnType.converterToCassandra)

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  override def readColumnValues(data: UniqueCassandraRow, buffer: Array[Any]) = {
    val columnName =  data.row.columnNames
    val columns = columnNames.map(table.columnByName)
    val converters = columns.map(_.columnType.converterToCassandra)
    for ((c, i) <- columnNames.zipWithIndex) {
      val value = data.row.getRaw(c)
      val convertedValue = converters(i).convert(value)
      buffer(i) = convertedValue
    }
  }

}


object UniqueRowWriter {

  object Factory extends RowWriterFactory[UniqueCassandraRow] {
    override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) =
      new UniqueRowWriter(table, selectedColumns)
  }

}
*/
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
    if(CRow.get[Int]("objid") == 586988297){
              exit
    }
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
 
class DStreamFunctions[T](dstream: DStream[T]) extends Serializable {

   def sparkContext: SparkContext = dstream.context.sparkContext
 
  def conf = sparkContext.getConf

  /**
   * Performs [[com.datastax.spark.connector.writer.WritableToCassandra]] for each produced RDD.
   * Uses specific column names with an additional batch size.
   */
  def saveToCassandra2(
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
//import org.apache.kafka.common.errors.SerializationException




/*
 class DBObject extends Dynamic with Serializable {
  private val map = new HashMap[String, Any]
  def selectDynamic(name: String): Any = {
    return map(name.toLowerCase)
  }
  def updateDynamic(name:String)(value: Any) = {
    map(name.toLowerCase) = value
  }
  def addField(name:String, value: Any) = this.updateDynamic(name)(value)
  def print{
    map.foreach(v => println(s"${v._1}:${v._2}"))
  }
  implicit class EnrichedWithToTuple[A](elements: Seq[A]) {
     def toTuple: Product = elements.length match {
        case 2 => toTuple2
        case 3 => toTuple3
     }
    def toTuple2: (A, A) = elements match {case Seq(a, b) => (a, b) }
    def toTuple3 = elements match {case Seq(a, b, c) => (a, b, c) }
 }
 // CassandraRow 
 // def toTuple = 
  def isEmpty = this.map.isEmpty
  def toTuple = {
     var seq = this.map.values.toSeq
     seq.toTuple
  }
  
}


import com.datastax.spark.connector.cql.TableDef
import scala.reflect.ClassTag
import scala.reflect._
import scala.reflect.runtime.universe._
class DBObjectColumnMapper (implicit tag: TypeTag[DBObject]) extends ColumnMapper[DBObject] {
  val MaxFields = 20
   override def classTag = implicitly[ClassTag[DBObject]]
   private def indexedColumnRefs(n: Int) =
    (0 until n).map(IndexedColumnRef)
    private def indexedColumnNames(n: Int) =
   (1 until n).map{ num => (s"_${num}",IndexedColumnRef( num -1) )}
    
  override def columnMap(tableDef: TableDef): ColumnMap = {

    val GetterRegex = "_([0-9]+)".r
   // val cls = implicitly[ClassTag[T]].runtimeClass

    val constructor =
      indexedColumnRefs(MaxFields)

    val getters = indexedColumnNames(MaxFields).toMap

    val setters =
      Map.empty[String, ColumnRef]

    SimpleColumnMap(constructor, getters, setters)
  }

}
 object DBObject {
       implicit val columnMapper =
         new DBObjectColumnMapper
}
*/

 
// Generic GG Row Record -needs a Schema to parse the vals array
// GoldenGate produces 4 types of operation
// "I" - Insert
// "D" - Delete
// "U" - Update
// "P" - Update Primary Key
trait GGRowRecord {
  def op:String
  def time:DateTime
  def vals:Array[String]
  // Default implementation 
  def parseWith(schema:Schema): CassandraRow = {
     println("Insert Operation")
  println("vals = " + vals.mkString(" "))
    val seq = vals.view.zipWithIndex.flatMap{ 
         case (v, i) if (schema(i).skip == false && v != null) => {
           println("i = " + i)
           println("v = " + v)
           Some(schema(i).name -> TypeCast.castTo(v,schema(i).dataType))
         }
         case _ => None
      }
     println("seq = " + seq.mkString(" "))
    CassandraRow.fromMap(seq.toMap)
  }
}
object GGRowRecord{ 
     case class Insert( op:String, time:DateTime,  vals:Array[String])   extends GGRowRecord{}
     case class Delete ( op:String, time:DateTime,  vals:Array[String])  extends GGRowRecord{}
     case class Update ( op:String, time:DateTime,  vals:Array[String])  extends GGRowRecord{
      override def parseWith(schema:Schema): CassandraRow = {
           println("Update Operation")
           /*val tmp = vals.grouped(2).map(x => (x.head,x.tail.head))
           val seq = vals.grouped(2).map(x => (x.head,x.tail.head)).flatMap{
              case(name, v) if (schema(name).skip == false) => Some(name -> v)
               case _ => None
            }*/
           println("vals = " + vals.mkString(" "))
           val bla  = vals.grouped(2).toList
           println("bla = " + bla.mkString(" "))
            val tmp = vals.grouped(2).toList.map(x=> x.toList)
            println("tmp = " + tmp.mkString(" "))

           val seq = tmp.flatMap{
              case name :: v if (schema(name).skip == false && v != null) => Some(name -> v)
              case _ => None
            }
           println(seq.toMap)
           CassandraRow.fromMap(seq.toMap)
      }
    }
    private case class UpdatePK( op:String, time:DateTime,  vals:Array[String]) extends GGRowRecord{}
    
    def apply( op:String, time:DateTime,  vals:Array[String]) : GGRowRecord  = {
      val obj = op match  {
        case("I") => new Insert(op, time,vals)
        case("U") => new Update(op, time,vals)
        case("D") => new Delete(op, time,vals)
        case("P") => new UpdatePK(op, time,vals)
        case _ => throw new RuntimeException(s"Unsupported Golden Gate Record type: ${op}")
      }
      obj
       
    }
}


   
// Golden Gate Table stuff
case class Column(  var name:String = "", val dataType:UTE.util.DataType = Unknown, val skip:Boolean = false){
  name = name.toLowerCase()
} // Schema column

case class Schema(val columns: Array[Column]){
  def fieldNames: Array[String] = columns.map(_.name)
  private lazy val nameToColumn: Map[String, Column] = columns.map(f => f.name.toLowerCase() -> f).toMap
  lazy val toCPairs = columns.map{ case Column(name,dataType, skip)=> 
           val c_type = dataType match {
               case StringType => "text"
               case IntegerType => "bigint"
               case DateType =>  "timestamp"
               case _ => throw new RuntimeException(s"Unsupported C* type")
            } 
            name -> c_type
  }
  def apply(index: Integer): Column = this.columns(index)
  def apply(name: String): Column = {
  //  println("Schema, name = " + name)
    this.nameToColumn(name.toLowerCase())
  }
  def mutationToRow (mutation: Mutation): CassandraRow = {
   val schema =  this  
   //println(mutation)
   val seq = mutation match  {
       case x if(x.isInstanceOf[RowMutation])   => {
               x.asInstanceOf[RowMutation].getRow.getColumns.asScala.flatMap{  
              case (key, v) if (schema(key).skip == false && v != null) => {
               // println("i = " + key)
               // println("v = " + v.getValue)
                Some(key.toLowerCase() -> TypeCast.castTo(v.getValue.asInstanceOf[String] , schema(key).dataType))
              }   
              case _ => None
            }
       }
       case _ => throw new Exception("Unsupported Mutation Type")
   }
    if (seq.exists(_ == ("objid", 586988297))){
         println("586988297!!!")   
         println(mutation)
          println("seq = " + seq.mkString(" "))
         //exit
    }
  //  println("seq = " + seq.mkString(" "))
    CassandraRow.fromMap(seq.toMap)
}
}
case class GGTable( val src_name:String, val target_name:String, val schema:Schema){
 
}
// TODO: This should obviously be in its own file
object Config {
  val sparkMasterHost = "127.0.0.1"
  val cassandraHost = "ec2-54-88-81-221.compute-1.amazonaws.com"
  val cassandraKeyspace = "demo"
  val cassandraCfCounters = "event_counters"
  val cassandraCfEvents = "event_log"
  val zookeeperHost = "52.4.197.159:2181"
  val kafkaHost = "52.4.197.159:9092"
  val table = "testTable"
  val schema = "testSchema"
  //val kafkaTopic =  KafkaUtil.genericTopic(schema, table)
  val kafkaTopic =  "SA_table_blg_argmnt_generic"

  //val kafkaTopic = "test_topic13"
 // val kafkaTopics = List[String]("table_blg_argmnt", "table_customer", "table_contact")
   val kafkaTopics = List[String]("table_blg_argmnt", "table_customer")
  //val kafkaConsumerGroup = "1spark-streaming-te

   
  val kafkaConsumerGroup = "spark-streaming-test-" + new Random().nextInt(100000)
  val tcpHost = "localhost"
  val tcpPort = 9999
  val topicsToTables = Map("SA_table_blg_argmnt_generic" -> "Agreement", "table_customer" -> "Customer", "table_contact" -> "Contact")//TODO: vals should be a Tuple?
  val tables = Map(
            "Agreement" -> 
                  GGTable(
                         "Agreement", "agreement",
                            Schema( Array(
                                Column("OBJID", IntegerType, false),
                                Column("Dev", IntegerType, false),
                                Column("Last_Update", DateType, false),
                                Column("Hier_Name_Ind", IntegerType, false),
                                Column("Name", StringType, false),
                                Column("S_Name", StringType, false),
                                Column("Status", StringType, false),
                                Column("Description", StringType, false),
                                Column("S_Description", StringType, false),
                                Column("Blg_Evt_Gen_Sts", IntegerType,false),
                                Column("Bar_Id", StringType, false),
                                Column("S_Bar_Id", StringType, false),
                                Column("Status_Date", DateType, false),
                                Column("BLG_ARGMNT2FIN_ACCNT", IntegerType, false),
                                Column("BA_PARENT2BUS_ORG", IntegerType, false),
                                Column("BA_CHILD2BUS_ORG", IntegerType, false),
                                Column("BLG_ARGMNT2PAY_MEANS", IntegerType, false),
                                Column("PRIMARY_BLG_ARGMNT2SITE", IntegerType, false),
                                Column("PRIMARY_BLG_ARGMNT2E_ADDR", IntegerType, false),
                                Column("BLG_STATUS2HGBST_ELM", IntegerType, false),
                                Column("X_RCIS_ID", StringType, false),
                                Column("BLG_ARGMNT2BOH_BE", IntegerType, false),
                                Column("X_DECLINE_OL_BILL", IntegerType, false),
                                Column("X_DECLINE_OL_BILL_DATE", DateType, false),
                                Column("X_EMAIL", StringType, false),
                                Column("X_WEB_USER_IND", IntegerType, false),
                                Column("X_OLB_IND", StringType, false),
                                Column("X_LAST_PAPER_CHRG_NOTIFY_DATE", DateType, false),
                                Column("X_OLB_CHARGE_WAIVE_IND", StringType, false),
                                Column("X_OLB_CHARGE_WAIVE_RSN", StringType, false),
                                Column("X_OLB_DATE", DateType, false),
                                Column("X_LAST_UPDATE_BILL_TYPE", DateType, false),
                                Column("X_OLB_WAIVE_RSN2HGBST_ELM", IntegerType, false)            
                            ))
                         ),
              "table_customer" -> 
                  GGTable("table_customer", "customer",
                            Schema( Array(
                                Column("Id", IntegerType, false),
                                Column("CUSTOMER_ID", StringType, false),
                                Column("S_CUSTOMER_ID", StringType, false),
                                Column("NAME", StringType, false),
                                Column("S_NAME", StringType, false),
                                Column("ACQUISITION_DATE", DateType, false),
                                Column("RSC_PL_CD", StringType, false),
                                Column("TYPE", StringType, false),
                                Column("SUBTYPE", StringType, false),
                                Column("RANK", IntegerType, false),
                                Column("PRIVACY_PREF", StringType, false),
                                Column("MARKET_CHANNEL", StringType, false),
                                Column("DEV", IntegerType, false),
                                Column("OWNED_ORG2BUS_ORG", IntegerType, false),
                                Column("CUSTOMER2ROLLUP", IntegerType, false),
                                Column("CUSTOMER2BOH_BE", IntegerType, false),
                                Column("CUSTOMER2CURRENCY", IntegerType, false),
                                Column("CUSTOMER2HGBST_ELM", IntegerType, false),
                                Column("CUST_TYPE2HGBST_ELM", IntegerType, false),
                                Column("X_RCIS_CREATION_DT", DateType, false),
                                Column("X_CONV_IND", IntegerType, false),
                                Column("X_WEB_SITE", StringType, false),
                                Column("X_EMPLOYEE_ID", StringType, false),
                                Column("X_SPECIAL_DISC", StringType, false),
                                Column("X_BUSINESS_TYPE", StringType, false),
                                Column("X_COMP_SIZE", StringType, false),
                                Column("X_REVENUE", StringType, false),
                                Column("X_INDUSTRY_TYPE", StringType, false),
                                Column("X_SERVICE_MODEL", StringType, false),
                                Column("X_SUB_MARKET", StringType, false),
                                Column("X_PPV_PIN", StringType, false),
                                Column("X_RC_FREQ", StringType, false),
                                Column("X_V21_CYCLE_BAN", StringType, false),
                                Column("X_BCB_IND", IntegerType, false),
                                Column("ACT_CRDT2CCLASS_INST", IntegerType, false),
                                Column("SUB_TYPE2HGBST_ELM", IntegerType, false),
                                Column("MARKET2HGBST_ELM", IntegerType, false),
                                Column("X_COMP_SIZE2HGBST_ELM", IntegerType, false),
                                Column("X_REVENUE2HGBST_ELM", IntegerType, false),
                                Column("X_INDUSTRY_TYPE2HGBST_ELM", IntegerType, false),
                                Column("X_SERVICE_MODEL2HGBST_ELM", IntegerType, false),
                                Column("X_BUSINESS_TYPE2HGBST_ELM", IntegerType, false),
                                Column("X_CUSTOMER2X_COMP_CODE_NAME", IntegerType, false),
                                Column("X_SPECIAL_DISC2HGBST_ELM", IntegerType, false),
                                Column("X_RC_FREQ2HGBST_ELM", IntegerType, false)
                             ))
                         )                  
         )
}

//case class GGRowRecrod(bucket:Long, time:Date, name:String, count:Long)

case class GGRowRecrodCount(bucket:Long, name:String, count:Long)

object StreamConsumer {
  

  def setup() : (SparkContext, StreamingContext, CassandraConnector) = {
    //TODO: this is a debug conf... probalby want an empty conf instead
    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", Config.cassandraHost)
      .set("spark.cleaner.ttl", "3600")
     .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)

    // Connect to the Spark cluster:
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(1))
    val cc = CassandraConnector(sc.getConf)
    createSchema(cc, Config.cassandraKeyspace)
      val collection = sc.parallelize(Seq(("cat", 30), ("fox", 40))) 
     
      //collection.saveToCassandra("test", "words", SomeColumns("word", "count"))
    return (sc, ssc, cc)
  }

  def parseDate(str:String) : DateTime = {

    //return javax.xml.bind.DatatypeConverter.parseDateTime(str).getTime()
    //return DateTime.parse(str )
    return DateTime.parse(str,    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))
    //val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")//2015-05-13 16:19:36.728160
    //eturn fmt.parse(str )
  }

  def minuteBucket(d:Date) : Long = {
    return d.getTime() / (60 * 1000)
  }
  // Parse Kafka message (a string) into a record of (op, time, col vals)
  def parseMessage(msg:String) : GGRowRecord = {
    val arr = msg.split(";")
    //val time = parseDate(arr.last)
    val time = DateTime.now
    val op = arr.head
    val vals = arr.slice(1,arr.length - 2)
    //  println("Vals = " + vals.mkString(" "))
    //println("Message = " + GGRowRecord(op, time, vals))
    return GGRowRecord(op, time, vals)
  }

  def createSchema(cc:CassandraConnector, keySpaceName:String) = {
    cc.withSessionDo { session =>
      Config.tables.foreach{ case (name,table) =>
        session.execute(s"DROP TABLE IF EXISTS ${keySpaceName}.${table.target_name};")
         // NOTE: This is for testing. Creating tables dynamically is not a good idea
         var cql = "CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.${table.target_name} (${table.schema.toCPairs.map{case(k,v) => s"${k} ${v}"}.mkString(", ")}" +
                      s", PRIMARY KEY(${table.schema.toCPairs(0)._1}));"
          println(cql)
                      session.execute(cql);
           session.execute("CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.count (word text, count bigint, " +
                      s"PRIMARY KEY(word));")
                      session.execute("CREATE TABLE IF NOT EXISTS " +
                      s"${keySpaceName}.words (word text, count bigint, " +
                      s"PRIMARY KEY(word));")
        
      }
    }
  }
  def getTable(topic:String) : GGTable  = {
   // var table = Config.tables.get(Config.topicsToTables.get(topic).get)
    //table match {
     // case Some(t) => t
      //case None => throw new Exception("Cannot find table")
   // }
  // Config.tables.get(topic).flatMap(m1.get(_))
    val tmp_topic = Config.topicsToTables.get(topic).get
   val table = Config.topicsToTables.get(topic).flatMap(Config.tables.get(_))
   table match {
      case Some(t) => t
      case None => throw new Exception(s"Cannot find table for topic: $tmp_topic $topic" )
    }
  }
  
  def process(ssc : StreamingContext, input : DStream[Array[Byte]], topic: String, sc: SparkContext) {// MsgT
    // for testing purposes you can use the alternative input below
     println(s" process topic: ${topic}")
    input.print()
    //val inputStr = input.map{ b => Hex.encodeHex(b).toString}
    // val inputStr = input.map{ b => new String(b)}
     val inputStr = input.map(b => b.map("%02x".format(_)).mkString)
     println("As Hex String")
    inputStr.print()
    println (s"input size = ${input.count().foreach(rdd => println(rdd)) }")
    
    var encrypt = true;
    var writeToC = true;
   // var bla = input.map(string => new EncryptedMessage(string.getBytes())).map(Encryptor.DecryptToString(_)).map(parseMessage)
   val bla = input.map(msg => new String(msg))
   println("Before Bla")
   bla.print

    val parsedGGRowRecrods =  encrypt match {
      // TODO: For comprehension? The true case is uuugly 
      case true => input.map(msg => parseMessage(new String(msg)))
      case false => inputStr.map(parseMessage)
    } 
    if(writeToC){
     var table =  getTable(topic)
     println("table = " + table)
     var rows = parsedGGRowRecrods.flatMap{
       case x:GGRowRecord.Insert => Some(x.parseWith(table.schema))
       case x:GGRowRecord.Update => None
     }
      println(s" Before SaveToCassandra ${topic}")
    // rows.saveToCassandra(Config.cassandraKeyspace, table.target_name, SomeColumns(table.schema.fieldNames:_*))
        println(s" After SaveToCassandra ${topic}")
    }
 }
  implicit def toDStreamFunctions[T: ClassTag](ds: DStream[T]): DStreamFunctions[T] =
    new DStreamFunctions[T](ds)
    
    
  def processNew(ssc : StreamingContext, input : DStream[Mutation], topic: String, sc: SparkContext) {// MsgT
    // for testing purposes you can use the alternative input below
     println(s" process topic: ${topic}")
   // input.print()
     //val inputStr = input.map(b => b.toString())
     //println("Mutation String")
  //  inputStr.print()
   // println (s"input size = ${input.count().foreach(rdd => println(rdd)) }")
    
    var writeToC = true;
   //val bla = input.map(msg => new String(msg))
   //bla.print
   //var desrialzier = new AvroDeserializer
    //val parsedGGRowRecrods =  input.map(desrialzier.deserialize)
    input.foreach(record => println(record))
    if(writeToC){
     var table =  getTable(topic)
    // print("table = " + table)
    // var rows = input.flatMap{
      // case x:InsertMutation => Some(x.parseWith(table.schema))
       //case x:UpdateMutation.Update => None
    // }
     var rows = input.map(table.schema.mutationToRow)
     // println(s" Before SaveToCassandra ${topic}")
    // rows.saveToCassandra(Config.cassandraKeyspace, table.target_name, SomeColumns(table.schema.fieldNames:_*))
      rows.saveToCassandra2(Config.cassandraKeyspace, table.target_name)
      //rows.sa
     /* rows.foreachRDD { rdd =>
              rdd.foreach { row => row.to  saveToCassandra(Config.cassandraKeyspace, table.target_name, SomeColumns)
   
       }
     }*/
          
          //=> row.saveToCassandra(Config.cassandraKeyspace, table.target_name, SomeColumns(row.)))
       // println(s" After SaveToCassandra ${topic}")
    }
 }

  
}


  
object KafkaConsumer{
  
  def newConsumer(args: Array[String]) {
    val (sc, ssc, cc) = StreamConsumer.setup()
    //mutationConsumer = 
  }
  def main(args: Array[String])  {

    val (sc, ssc, cc) = StreamConsumer.setup()
    println(s"Consumer Group = ${Config.kafkaConsumerGroup}")
   val kafkaParams: Map[String, String] = Map("group.id" -> Config.kafkaConsumerGroup, 
                                              "zookeeper.connect" -> Config.zookeeperHost,
                                              "consumer.forcefromstart"  -> "true", 
                                              "auto.offset.reset" -> "smallest",
                                              "value.serializer" -> "com.goldengate.delivery.handler.kafka.util.KafkaSecureByteArraySerializer",
                                              "crypto.key_provider" -> "test",
                                              "crypto.encryptor" -> "avro"
                                              )
       var topic = Config.kafkaTopic
       var topic_set: Set[String] =  Set("bla","bla2")
        val input = KafkaUtils.createStream [String, Mutation, StringDecoder, KafkaAvroMutationDecoder](// MsgT
        ssc,
        kafkaParams,
       // Config.zookeeperHost,  
        //Config.kafkaConsumerGroup,
       Map(topic -> 1),
       StorageLevel.MEMORY_AND_DISK_SER_2
       ).map(_._2)

       def conf = sc.getConf
        val connector: CassandraConnector = CassandraConnector(conf)

       println("Before Process")
       println("Before Process2")
       StreamConsumer.processNew(ssc, input, topic, sc)
        println("After Process")
       
        
      
  //   })
        
      sys.ShutdownHookThread {
         ssc.stop(true, true)
       }

        
       ssc.start()
       ssc.awaitTermination()
    }
}


object TCPConsumer {
  def main(args: Array[String]) {
    val (sc, ssc, cc) = StreamConsumer.setup()
    val input = ssc.socketTextStream(Config.tcpHost, Config.tcpPort)
    //StreamConsumer.process(ssc, input)
    sys.ShutdownHookThread {
      ssc.stop(true, true)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

object EventGenerator {

  val eventNames = Array("thyrotome", "radioactivated", "toreutics", "metrological",
    "adelina", "architecturally", "unwontedly", "histolytic", "clank", "unplagiarised",
    "inconsecutive", "scammony", "pelargonium", "preaortic", "goalmouth", "adena",
    "murphy", "vaunty", "confetto", "smiter", "chiasmatype", "fifo", "lamont", "acnode",
    "mutating", "unconstrainable", "donatism", "discept")

  def currentTimestamp() : String = {
    val tz = TimeZone.getTimeZone("UTC")
    val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    sdf.setTimeZone(tz)
    val dateString = sdf.format(new java.util.Date)
    return dateString
  }

  def randomEventName() : String = {
    val rand = new Random(System.currentTimeMillis())
    val random_index = rand.nextInt(eventNames.length)
    return eventNames(random_index)
  }

  def generateEvent() : String = {
    // message in the form of "2014-10-07T12:20:08Z;foo;1"
    val eventCount = scala.util.Random.nextInt(10).toString()
    return currentTimestamp() + ";" + randomEventName() + ";" + eventCount
  }
}

object KafkaProducer { 
 def main(args: Array[String]) {
   val props = new Properties()
   props.put("metadata.broker.list", Config.kafkaHost)
   props.put("serializer.class", "kafka.serializer.StringEncoder")
   println("")
   val config = new ProducerConfig(props)
   val producer = new Producer[String, String](config)

   while(true) {
     val event = EventGenerator.generateEvent();
     println(event)
     producer.send(new KeyedMessage[String, String](Config.kafkaTopic, event))
     Thread.sleep(100)
   }
 }
}

object TcpProducer {
  def main(args: Array[String]) {
    val server = new ServerSocket(Config.tcpPort)

    while (true) {
      val socket = server.accept()
      val outstream = new PrintStream(socket.getOutputStream())
      while (!outstream.checkError()) {
        val event = EventGenerator.generateEvent();
        println(event)
        outstream.println(event)
        Thread.sleep(100)
      }
      socket.close()
    }
  }
}

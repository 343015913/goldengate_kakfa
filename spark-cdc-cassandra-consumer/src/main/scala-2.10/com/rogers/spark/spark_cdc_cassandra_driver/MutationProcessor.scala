package com.rogers.spark.spark_cdc_cassandra_driver

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

import com.rogers.cdc.api.mutations.Mutation
import com.rogers.cdc.api.mutations.Mutation._

import com.rogers.spark.spark_cdc_cassandra_driver.util._

/**
 * @author eugene.miretsky
 */
abstract class MutationProcessor(tableMap: String => GGTable) {
  type T // abstract methods with type parameters are tricky. This is a work around as per (http://stackoverflow.com/questions/18507362/scala-error-implementing-abstract-method-with-type-parameter)
  def process(ssc: StreamingContext, input: DStream[T], topic: String, sc: SparkContext)
  implicit def toDStreamFunctions[T: ClassTag](ds: DStream[T]): DStreamFunctions[T] = new DStreamFunctions[T](ds)
}

import com.github.nscala_time.time.Imports.DateTime
import com.github.nscala_time.time.Imports.DateTimeFormat

class AvroStringMutationProcessor(tableMap: String => GGTable) extends MutationProcessor(tableMap) {
  type T = Mutation

  def process(ssc: StreamingContext, input: DStream[T], topic: String, sc: SparkContext) { // MsgT
    import com.rogers.spark.spark_cdc_cassandra_driver.util.DStreamFunctions
    // for testing purposes you can use the alternative input below
    println(s" process topic: ${topic}")

    var writeToC = true;

    //input.foreach(record => println(record))
    if (writeToC) {
      var table = tableMap(topic)

      var rows = input.map(table.schema.mutationToRow(_))

      rows.saveToCassandraSimple(Config.cassandraKeyspace, table.target_name)

    }
  }
}

class StringMutationProcessor(tableMap: String => GGTable) extends MutationProcessor(tableMap) {
  type T = Array[Byte]
  private def parseMessage(msg: String): GGRowRecord = {
    val arr = msg.split(";")
    //val time = parseDate(arr.last)
    val time = DateTime.now
    val op = arr.head
    val vals = arr.slice(1, arr.length - 2)
    //  println("Vals = " + vals.mkString(" "))
    //println("Message = " + GGRowRecord(op, time, vals))
    return GGRowRecord(op, time, vals)
  }
  def process(ssc: StreamingContext, input: DStream[T], topic: String, sc: SparkContext) { // MsgT
    // for testing purposes you can use the alternative input below
    println(s" process topic: ${topic}")
    input.print()

    /*    val inputStr = input.map(b => b.map("%02x".format(_)).mkString)
    println("As Hex String")
    inputStr.print()
    println(s"input size = ${input.count().foreach(rdd => println(rdd))}")
*/
    var encrypt = true;
    var writeToC = true;
    // var bla = input.map(string => new EncryptedMessage(string.getBytes())).map(Encryptor.DecryptToString(_)).map(parseMessage)
    /*val bla = input.map(msg => new String(msg))
    println("Before Bla")
    bla.print*/
    /*
    val parsedGGRowRecrods = encrypt match {
      // TODO: For comprehension? The true case is uuugly
      case true  => input.map(msg => parseMessage(new String(msg)))
      case false => inputStr.map(parseMessage)
    }
    if (writeToC) {
      var table = tableMap(topic)
      println("table = " + table)
      var rows = parsedGGRowRecrods.flatMap {
        case x: GGRowRecord.Insert => Some(x.parseWith(table.schema))
        case x: GGRowRecord.Update => None
      }

      */
    //println(s" Before SaveToCassandra ${topic}")
    // rows.saveToCassandra(Config.cassandraKeyspace, table.target_name, SomeColumns(table.schema.fieldNames:_*))
    // println(s" After SaveToCassandra ${topic}")
    //}
  }
}
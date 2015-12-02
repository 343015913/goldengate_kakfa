
package com.rogers.spark.spark_cdc_cassandra_driver

import java.util.Date
import java.util.Random

import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import com.datastax.spark.connector.cql.CassandraConnector
import com.github.nscala_time.time.Imports.DateTime
import com.github.nscala_time.time.Imports.DateTimeFormat
import com.rogers.cdc.api.mutations.Mutation
import com.rogers.cdc.kafka.serializers.KafkaAvroMutationDecoder
import com.rogers.spark.spark_cdc_cassandra_driver.util.DStreamFunctions

import kafka.serializer.StringDecoder

// TODO: This should obviously be in its own file
object Config {
  val sparkMasterHost = "127.0.0.1"
  val cassandraHost = "ec2-54-88-81-221.compute-1.amazonaws.com"
  val cassandraKeyspace = "demo"
  val zookeeperHost = "52.4.197.159:2181"
  val kafkaHost = "52.4.197.159:9092"
  val kafkaConsumerGroup = "spark-streaming-test-" + new Random().nextInt(100000)
}
case class GGRowRecrodCount(bucket: Long, name: String, count: Long)

object StreamConsumer {

  def setup(): (SparkContext, StreamingContext, CassandraConnector) = {
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
  def createSchema(cc: CassandraConnector, keySpaceName: String) = {
    cc.withSessionDo { session =>
      Table.tables.foreach {
        case (name, table) =>
          session.execute(s"DROP TABLE IF EXISTS ${keySpaceName}.${table.target_name};")
          // NOTE: This is for testing. Creating tables dynamically is not a good idea
          var cql = "CREATE TABLE IF NOT EXISTS " +
            s"${keySpaceName}.${table.target_name} (${table.schema.toCPairs.map { case (k, v) => s"${k} ${v}" }.mkString(", ")}" +
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
  def getTable(topic: String): GGTable = {

    val table = Table.topicsToTables.get(topic).flatMap(Table.tables.get(_))
    table match {
      case Some(t) => t
      case None    => throw new Exception(s"Cannot find table for topic: $Config.topicsToTables.get(topic).get $topic")
    }
  }

}

object KafkaConsumer {
  def main(args: Array[String]) {

    val (sc, ssc, cc) = StreamConsumer.setup()

    val kafkaParams: Map[String, String] = Map("group.id" -> Config.kafkaConsumerGroup,
      "zookeeper.connect" -> Config.zookeeperHost,
      "consumer.forcefromstart" -> "true",
      "auto.offset.reset" -> "smallest",
      "value.serializer" -> "com.goldengate.delivery.handler.kafka.util.KafkaSecureByteArraySerializer",
      "crypto.key_provider" -> "test",
      "crypto.encryptor" -> "avro")
    // var topic = Table.kafkaTopics

    var proc = new AvroStringMutationProcessor(StreamConsumer.getTable)

    // create a Stream for each topic.
    Table.kafkaTopics.foreach { topic =>
      val input = KafkaUtils.createStream[String, Mutation, StringDecoder, KafkaAvroMutationDecoder]( // MsgT
        ssc,
        kafkaParams,
        Map(topic -> 1),
        StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)

      def conf = sc.getConf
      val connector: CassandraConnector = CassandraConnector(conf)

      proc.process(ssc, input, topic, sc)
    }

    sys.ShutdownHookThread {
      ssc.stop(true, true)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

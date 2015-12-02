package com.rogers.spark.spark_cdc_cassandra_driver

import org.joda.time.DateTime

import com.datastax.spark.connector.CassandraRow
import com.rogers.spark.spark_cdc_cassandra_driver.util.TypeCast

// Generic GG Row Record -needs a Schema to parse the vals array
// GoldenGate produces 4 types of operation
// "I" - Insert
// "D" - Delete
// "U" - Update
// "P" - Update Primary Key
trait GGRowRecord {
  def op: String
  def time: DateTime
  def vals: Array[String]
  // Default implementation
  def parseWith(schema: Schema): CassandraRow = {
    println("Insert Operation")
    println("vals = " + vals.mkString(" "))
    val seq = vals.view.zipWithIndex.flatMap {
      case (v, i) if (schema(i).skip == false && v != null) => {
        println("i = " + i)
        println("v = " + v)
        Some(schema(i).name -> TypeCast.castTo(v, schema(i).dataType))
      }
      case _ => None
    }
    println("seq = " + seq.mkString(" "))
    CassandraRow.fromMap(seq.toMap)
  }
}
object GGRowRecord {
  case class Insert(op: String, time: DateTime, vals: Array[String]) extends GGRowRecord {}
  case class Delete(op: String, time: DateTime, vals: Array[String]) extends GGRowRecord {}
  case class Update(op: String, time: DateTime, vals: Array[String]) extends GGRowRecord {
    override def parseWith(schema: Schema): CassandraRow = {
      println("Update Operation")
      /*val tmp = vals.grouped(2).map(x => (x.head,x.tail.head))
           val seq = vals.grouped(2).map(x => (x.head,x.tail.head)).flatMap{
              case(name, v) if (schema(name).skip == false) => Some(name -> v)
               case _ => None
            }*/
      println("vals = " + vals.mkString(" "))
      val bla = vals.grouped(2).toList
      println("bla = " + bla.mkString(" "))
      val tmp = vals.grouped(2).toList.map(x => x.toList)
      println("tmp = " + tmp.mkString(" "))

      val seq = tmp.flatMap {
        case name :: v if (schema(name).skip == false && v != null) => Some(name -> v)
        case _ => None
      }
      println(seq.toMap)
      CassandraRow.fromMap(seq.toMap)
    }
  }
  private case class UpdatePK(op: String, time: DateTime, vals: Array[String]) extends GGRowRecord {}

  def apply(op: String, time: DateTime, vals: Array[String]): GGRowRecord = {
    val obj = op match {
      case ("I") => new Insert(op, time, vals)
      case ("U") => new Update(op, time, vals)
      case ("D") => new Delete(op, time, vals)
      case ("P") => new UpdatePK(op, time, vals)
      case _     => throw new RuntimeException(s"Unsupported Golden Gate Record type: ${op}")
    }
    obj

  }
}
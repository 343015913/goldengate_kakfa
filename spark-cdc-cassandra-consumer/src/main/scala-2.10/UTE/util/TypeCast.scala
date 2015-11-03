/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package UTE.util

import java.math.BigDecimal
import java.sql.{Timestamp, Date}
import com.github.nscala_time.time.Imports._

//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.sql.types._

/**
 * Utility functions for type casting
 */

sealed trait DataType  extends java.io.Serializable
object Unknown extends DataType
object ByteType extends DataType
object ShortType extends DataType
object IntegerType extends DataType
object LongType extends DataType
object FloatType extends DataType
object DoubleType extends DataType
object BooleanType extends DataType
object DecimalType extends DataType
object TimestampType extends DataType
object DateType extends DataType
object StringType extends DataType



object TypeCast {

  /**
   * Casts given string datum to specified type.
   * Currently we do not support complex types (ArrayType, MapType, StructType).
   *
   * @param datum string value
   * @param castType 
   */
  def castTo(datum: String, castType: DataType): Any = { 
     //   println("castTo val=" + datum)
   // println("castTo val=" + castType)
    castType match {  
      case  ByteType => datum.toByte //Is instance of - OK
      case  ShortType => datum.toShort // extraction on a class - FAIL (looking for value) 
      case  IntegerType => datum.toInt // Extraction on singelton - OK
      case  LongType => datum.toLong   // Is instance of a singelton - FAIL (looing for type)
      case  FloatType => datum.toFloat
      case  DoubleType => datum.toDouble
      case  BooleanType => datum.toBoolean
      case  DecimalType => new BigDecimal(datum.replaceAll(",", ""))
      case  TimestampType => Timestamp.valueOf(datum)
      case  DateType => DateTimeFormat.forPattern("YYYY-MM-DD:HH:mm:ss").parseDateTime(datum)
      case  StringType => datum
      case _ => throw new RuntimeException(s"Unsupported type")
    }
  }
 
  

  /**
   * Helper method that converts string representation of a character to actual character.
   * It handles some Java escaped strings and throws exception if given string is longer than one
   * character.
   *
   */
  @throws[IllegalArgumentException]
   def toChar(str: String): Char = {
    if (str.charAt(0) == '\\') {
      str.charAt(1)
       match {
        case 't' => '\t'
        case 'r' => '\r'
        case 'b' => '\b'
        case 'f' => '\f'
        case '\"' => '\"' // In case user changes quote char and uses \" as delimiter in options
        case '\'' => '\''
        case 'u' if str == """\u0000""" => '\u0000'
        case _ =>
          throw new IllegalArgumentException(s"Unsupported special character for delimiter: $str")
      }
    } else if (str.length == 1) {
      str.charAt(0)
    } else {
      throw new IllegalArgumentException(s"Delimiter cannot be more than one character: $str")
    }
  }
}

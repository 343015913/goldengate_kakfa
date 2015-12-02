package com.rogers.spark.spark_cdc_cassandra_driver

import scala.collection.JavaConverters.mapAsScalaMapConverter

import com.datastax.spark.connector.CassandraRow
import com.rogers.cdc.api.mutations.Mutation
import com.rogers.cdc.api.mutations.RowMutation
import com.rogers.spark.spark_cdc_cassandra_driver.util.DataType
import com.rogers.spark.spark_cdc_cassandra_driver.util.DateType
import com.rogers.spark.spark_cdc_cassandra_driver.util.IntegerType
import com.rogers.spark.spark_cdc_cassandra_driver.util.StringType
import com.rogers.spark.spark_cdc_cassandra_driver.util.TypeCast
import com.rogers.spark.spark_cdc_cassandra_driver.util.Unknown

//TODO: This is really a quick hack. Create a normal DSL once we know more about how we want to map Kafka Topics/ CDC Mutations to Cassandra tables  (really a DSL for mapping relational to C* data model)

// Golden Gate Table stuff
case class Column(var name: String = "", val dataType: DataType = Unknown, val skip: Boolean = false) {
  name = name.toLowerCase()
} // Schema column

case class Schema(val columns: Array[Column]) {
  def fieldNames: Array[String] = columns.map(_.name)
  private lazy val nameToColumn: Map[String, Column] = columns.map(f => f.name.toLowerCase() -> f).toMap
  lazy val toCPairs = columns.map {
    case Column(name, dataType, skip) =>
      val c_type = dataType match {
        case StringType  => "text"
        case IntegerType => "bigint"
        case DateType    => "timestamp"
        case _           => throw new RuntimeException(s"Unsupported C* type")
      }
      name -> c_type
  }
  def apply(index: Integer): Column = this.columns(index)
  def apply(name: String): Column = {
    this.nameToColumn(name.toLowerCase())
  }
  def mutationToRow(mutation: Mutation): CassandraRow = {
    val schema = this
    //println(mutation)
    val seq = mutation match {
      case x if (x.isInstanceOf[RowMutation]) => {
        x.asInstanceOf[RowMutation].getRow.getColumns.asScala.flatMap {
          case (key, v) if (schema(key).skip == false && v != null) => {

            Some(key.toLowerCase() -> TypeCast.castTo(v.getValue.asInstanceOf[String], schema(key).dataType))
          }
          case _ => None
        }
      }
      case _ => throw new Exception("Unsupported Mutation Type")
    }
    if (seq.exists(_ == ("objid", 586988297))) {
      println("586988297!!!")
      println(mutation)
      println("seq = " + seq.mkString(" "))
      //exit
    }
    CassandraRow.fromMap(seq.toMap)
  }
}
case class GGTable(val src_name: String, val target_name: String, val schema: Schema) {

}

object Table {
  // Maps Kafka topics to Cassandra tables - for now just 1 to 1 mapping
  val topicsToTables = Map("SA_table_blg_argmnt_generic" -> "Agreement", "SA_table_customer" -> "Customer", "table_contact" -> "Contact")
  // A list of Kafka Topics to consume
  val kafkaTopics = List[String]("SA_table_blg_argmnt_generic", "SA_table_customer")
  val tables = Map(
    "Agreement" ->
      GGTable(
        "Agreement", "agreement",
        Schema(Array(
          Column("OBJID", IntegerType, false),
          Column("Dev", IntegerType, false),
          Column("Last_Update", DateType, false),
          Column("Hier_Name_Ind", IntegerType, false),
          Column("Name", StringType, false),
          Column("S_Name", StringType, false),
          Column("Status", StringType, false),
          Column("Description", StringType, false),
          Column("S_Description", StringType, false),
          Column("Blg_Evt_Gen_Sts", IntegerType, false),
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
          Column("X_OLB_WAIVE_RSN2HGBST_ELM", IntegerType, false)))),
    "Customer" ->
      GGTable("Customer", "customer",
        Schema(Array(
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
          Column("X_RC_FREQ2HGBST_ELM", IntegerType, false)))))
}
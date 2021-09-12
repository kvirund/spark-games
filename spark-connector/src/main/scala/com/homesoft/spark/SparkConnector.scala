package com.homesoft.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._

class SparkConnector extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "excel"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = new ExcelRelation(sqlContext)
}
package com.homesoft.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class ExcelRelation(val sqlContext: SQLContext) extends BaseRelation with PrunedFilteredScan {
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = new ExcelRDD(sqlContext.sparkContext)

  override def toString: String = "Excel Spark Connector";

  override def schema: StructType = new StructType(Array(StructField("id", LongType), StructField("name", StringType)))
}

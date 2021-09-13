package com.homesoft.spark

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

import java.io.FileInputStream
import scala.collection.JavaConverters._

// TODO: instead of passing entire row to the Spark, it is better to filter it out inside the RDD
class ExcelRelation(val sqlContext: SQLContext, val parameters: Map[String, String]) extends BaseRelation with TableScan {
  override def buildScan(): RDD[Row] = {
    new ExcelRDD(sqlContext.sparkContext, parameters)
  }

  override def toString: String = "Excel Spark Connector";

  override def schema: StructType = {
    // TODO: error handling:
    //  - Closable resources
    //  - Missing parameters
    val workbook = new XSSFWorkbook(new FileInputStream(parameters("path")))
    val sheet = workbook.getSheet(parameters("sheetName"))

    // TODO: add handling of first and last row values
    val columns = sheet.getRow(0).iterator().asScala.map { cell => StructField(cell.getStringCellValue, StringType, true) }.toArray
    new StructType(columns)
  }
}

package com.homesoft.spark

import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, SparkContext, TaskContext}

import java.io.FileInputStream
import scala.collection.JavaConverters._

class ExcelRDD(sparkContext: SparkContext, val parameters: Map[String, String]) extends RDD[Row](sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    // TODO: error handling:
    //  - Closable resources
    //  - Missing parameters
    val workbook = new XSSFWorkbook(new FileInputStream(parameters("path")))
    val sheet = workbook.getSheet(parameters("sheetName"))

    val partition: ExcelPartition = split.asInstanceOf[ExcelPartition]
    val rows = Array.range(partition.firstRow, partition.lastRow).map(row => {
      val rowParameters = sheet.getRow(row).iterator().asScala.map(cell => {
        cell.getCellType match {
          case CellType.STRING => cell.getStringCellValue
          case CellType.NUMERIC => String.valueOf(cell.getNumericCellValue)
          case _ => throw new RuntimeException(s"Unexpected cell type ${cell.getCellType.toString}")
        }
      }).toArray

      rowParameters
    })

    val result = rows.map(row => Row(row: _*))

    result.iterator
  }

  override protected def getPartitions: Array[Partition] = {
    // TODO: error handling:
    //  - Closable resources
    //  - Missing parameters
    val workbook = new XSSFWorkbook(new FileInputStream(parameters("path")))
    val sheet = workbook.getSheet(parameters("sheetName"))

    // TODO: Split rows into partitions
    Array(new ExcelPartition(1 + sheet.getFirstRowNum, 1 + sheet.getLastRowNum))
  }
}

package com.homesoft.spark

import org.apache.spark.Partition

class ExcelPartition(val firstRow: Int, val lastRow: Int) extends Partition {
  override def index: Int = 0
}

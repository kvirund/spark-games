package com.homesoft.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

class ExcelRDD(sparkContext: SparkContext) extends RDD[Row](sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[Row] = Iterator(Row(1L, "Sample"))

  override protected def getPartitions: Array[Partition] = Array(
    new Partition {
      override def index: Int = 0
    }
  )
}

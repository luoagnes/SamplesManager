package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.apache.spark.sql.{DataFrame, SQLContext}

class DataFrameSqlTransform(id: String) extends UnaryNode[DataFrame, DataFrame](id) {

    override def doExecute(input: DataFrame): DataFrame = {
      val sqlStr = getPropertyOrThrow("sql")
      input.createOrReplaceTempView("tmp_table")
      val sqlContext = new SQLContext(SparkRegistry.spark.sparkContext)
      sqlContext.sql(sqlStr)
    }


}

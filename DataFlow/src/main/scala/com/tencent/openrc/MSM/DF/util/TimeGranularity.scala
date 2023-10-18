package com.tencent.openrc.MSM.DF.util

object TimeGranularity extends Enumeration{
  type TimeGranularity = Value
  val MINUTE, HOUR, DAY, WEEK, MONTH, YEAR = Value
}

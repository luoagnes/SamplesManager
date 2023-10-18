package com.tencent.openrc.MSM.DF.util


import scala.collection.mutable

class GenericArgParser(args: Array[String]) {

  val argsMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()

  for (arg <- args) {
    val arr = arg.split("=", 2)
    if (arr.size > 1) {
      argsMap.put(arr(0).trim, arr(1).trim)
    }
  }

  def getStringValueOrThrow(argName: String, errorMessage: Option[String] = None): String = {
    val realErrorMessage = errorMessage.getOrElse("missing " + argName)
    argsMap.getOrElse(argName, throw new RuntimeException(realErrorMessage))
  }

  def getStringValueOption(argName: String): Option[String] = {
    argsMap.get(argName)
  }

  def getStringValue(argName: String, defaultVal: String = ""): String = {
    argsMap.getOrElse(argName, defaultVal)
  }

  def getIntValue(argName: String, defaultVal: Int = 0): Int = {
    val v = argsMap.get(argName)
    if (v.isDefined) v.get.toInt else defaultVal
  }

  def getDoubleValue(argName: String, defaultVal: Double = 0.0): Double = {
    val v = argsMap.get(argName)
    if (v.isDefined) v.get.toDouble else defaultVal
  }

  def getCommaSplitArrayValue(argName: String,
                              defaultVal: Array[String] = Array()): Array[String] = {
    val v = argsMap.get(argName)
    if (v.isDefined) v.get.split(",") else defaultVal
  }

  override def toString: String = {
    argsMap.mkString("\n")
  }
}


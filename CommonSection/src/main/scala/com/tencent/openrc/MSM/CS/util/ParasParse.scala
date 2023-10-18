package com.tencent.openrc.MSM.CS.util

import org.apache.commons.cli
import org.apache.commons.cli.{CommandLine, ParseException}

object ParasParse {
  def parseArgs(args: Array[String], AllparasList:Array[(String, String)], RequiredParas:Array[String]=null): Boolean = {
    val formatstr = "gmkdir [-p][-v/--verbose][--block-size][-h/--help] DirectoryName"

    val RequiredItems=if(RequiredParas == null) AllparasList.map(_._1) else RequiredParas
    val ops = new cli.Options()

    for(parasObj <- AllparasList){
      val parasName=parasObj._1
      val parasDesc=parasObj._2
      ops.addOption(parasName, true, parasDesc)

    }

    val parse = new cli.PosixParser()
    val formatter = new cli.HelpFormatter()
    var result: CommandLine = null
    try {
      result = parse.parse(ops, args)
    } catch {
      case e: ParseException => formatter.printHelp(formatstr, ops)
        return false
    }

    for(paras <- RequiredItems){
      if (result.hasOption(paras)) {
        val NegMapUpPath = result.getOptionValue(paras)
        println("NegMapUpPath : " + NegMapUpPath)
      }
    }

    true // succeed to parse command line.
  }

}

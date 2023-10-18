package com.tencent.openrc.MSM.DF.driver

import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import com.tencent.openrc.MSM.DF.pipeline.{Pipeline, Task, WorkflowDefineException, XmlWorkflowBuilder}
import com.tencent.openrc.MSM.DF.util.{GenericArgParser, SparkRegistry, TimeGranularity, TimeUtil}
import com.tencent.openrc.MSM.DF.util.SparkRegistry.sc

import java.util.concurrent.{ExecutorCompletionService, ExecutorService, Executors}
import scala.util.{Failure, Success, Try}

/**
 * 对于adhoc执行的任务，可以直接使用该类来完成基本的任务。
 * 在命令行输入参数configFile=xxx即可
 * 如有特殊的业务逻辑，可以继承该类。
 */

class DataFlowApp {

    private val log = LoggerFactory.getLogger(this.getClass)

    var pipelines: Array[Pipeline] = _

    var pipelineParallelism: Int = 1

    lazy val pool: ExecutorService = Executors.newFixedThreadPool(pipelineParallelism)
    lazy val completionService = new ExecutorCompletionService[Boolean](pool)
    lazy val builder = new XmlWorkflowBuilder()

    var completeDonePathReplaced: String = _

    def init(args: Array[String]): Boolean = {
      // parse 命令行参数
      val argParser = new GenericArgParser(args)
      // scalastyle:off println
      println("args:" + argParser)
      println("debug1")
      log.isInfoEnabled
      // 获取workflow的描述文件
      val configFiles = argParser.getCommaSplitArrayValue("configFile")
        .map(s => s.trim)
        .filter(s => !s.isEmpty)
      println("debug2: " + configFiles.mkString(","))
      if (configFiles.isEmpty) {
        throw WorkflowDefineException("no config files were found")
      }
      pipelineParallelism = argParser.getIntValue("pipelineParallelism", 1)

      val regression = argParser.getIntValue("regression", 1)
      log.info(s"regression = ${regression}")

      val dataTime = argParser.getStringValueOrThrow("dataTime")

      pipelines = (0 until regression).flatMap(index => {
        val granularity = {
          if (dataTime.length == 8) {
            TimeGranularity.DAY
          } else if (dataTime.length == 10) {
            TimeGranularity.HOUR
          } else if (dataTime.length == 12) {
            TimeGranularity.MINUTE
          } else {
            throw new RuntimeException(s"wrong dataTime format: ${dataTime}")
          }
        }

        val dataTimeInProgress =
          TimeUtil.getLastDateTime(dataTime, granularity, granularity, regression - index - 1)
        log.info(s"dataTimeInProgress = ${dataTimeInProgress}")

        println("configFiles.length: " +configFiles.length.toString)
        println(configFiles)
        configFiles.flatMap(file => {
//          Try {
            println("start to build by file")
            val job = builder.buildByFile(file)

            if (job.isDefined) {
              println("build configure over !!!")
              val pipeline = job.get.pipeline
              argParser.argsMap.map(kv => {
                println("kv._1, kv._2:")
                println(kv._1, kv._2)
                pipeline.addArg(kv._1, kv._2)
              })
              pipeline.addArg("dataTime", dataTimeInProgress)
              pipeline.addArg("beginDataTime", dataTime)
              pipeline.init()
              Some(pipeline)
            } else {
              None
            }
//          } match {
//            case Success(r) => r
//            case Failure(e) => {
//              println(s"file: ${file}, parse failed and ignored")
//              e.printStackTrace()
//              None
//            }
//          }
        })
      }).toArray

      val completeDonePath = pipelines(0).getProperty("generate_training_data.complete_done_path")
      if (completeDonePath.isDefined) {
        val beginDataTimeTs = TimeUtil.strToTs(dataTime)
        completeDonePathReplaced =
          TimeUtil.replaceCurrentTimeWildcards(completeDonePath.get, beginDataTimeTs)
        println(s"Complete done file: ${completeDonePathReplaced}")
      }
      true
    }

    def run(): Unit = {
      beforeAppRun()
      val runResult =
        pipelines
          .map(pipeline => completionService.submit(createNewTask(pipeline)))
          .map(f => completionService.take().get())
          .reduce(_ && _)
      afterAppRun(runResult)
    }

    def createNewTask(pipeline: Pipeline): Task = {
      new Task(pipeline)
    }

    def beforeAppRun(): Unit = {
      if (completeDonePathReplaced != null) {
        deleteCompleteDoneFile()
      }
    }

    def afterAppRun(runResult: Boolean): Unit = {
      pool.shutdown()
      if (!runResult) {
        throw new RuntimeException("Run failed")
      }

      if (completeDonePathReplaced != null) {
        createCompleteDoneFile()
      }
    }

    def deleteCompleteDoneFile(): Unit = {
      val successFile = new Path(completeDonePathReplaced + "/_SUCCESS")
      val fs = successFile.getFileSystem(SparkRegistry.sc.hadoopConfiguration)
      if (fs.exists(successFile)) {
        val res = fs.delete(successFile)
        if (!res) {
          throw new RuntimeException(
            "delete final complete success file " + successFile + " failed")
        }
        println(s"delete final complete success file: ${successFile}")
      }
    }

    def createCompleteDoneFile(): Unit = {
      val successFile = new Path(completeDonePathReplaced + "/_SUCCESS")
      val fs = successFile.getFileSystem(SparkRegistry.sc.hadoopConfiguration)
      if (!fs.exists(successFile)) {
        val os = fs.create(successFile)
        os.close
        println(s"touch final complete success file: ${successFile}")
      }
    }
  }

  object DataFlowApp {

    def main(args: Array[String]): Unit = {
      sc.setLogLevel("INFO")
      val app = new DataFlowApp()
      if (app.init(args)) {
        println("debug3:")
        app.run()
      }
    }

}

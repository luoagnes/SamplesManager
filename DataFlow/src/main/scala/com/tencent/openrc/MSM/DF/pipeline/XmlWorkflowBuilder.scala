/*
 * Copyright (c) 2018 Tencent Inc.
 * All rights reserved.
 * Author: alexiszhu <alexiszhu@tencent.com>
 * Modified by haoxiliu
 */
package com.tencent.openrc.MSM.DF.pipeline

import java.io.InputStream

import scala.collection.mutable
import scala.io.Source
import scala.reflect.runtime.universe
import scala.xml.{Node, XML}
// scalastyle:off
import scala.reflect.runtime.universe._
import com.tencent.openrc.MSM.DF.pipeline.node.{BaseNode, UnaryNode,GeneratorNode,MultiInputsNode}

class XmlWorkflowBuilder {

  val mirror: universe.Mirror = runtimeMirror(getClass.getClassLoader)
  val nodeMap: mutable.HashMap[String, BaseNode] = mutable.HashMap[String, BaseNode]()

  private val ID_LITERAL = "@id"
  private val INPUT_LITERAL = "@input"

  def buildByString(content: String, configOnly: Boolean = false): Option[Job] = {
    nodeMap.clear()
    val xml = XML.loadString(content)
    println("xml load success ! label is: "+xml.label)
    val jobNodes = xml.filter(n => n.label == "job")
    println("jobNodes num is: "+jobNodes.length.toString)
    if (jobNodes.nonEmpty) {
      val job = jobNodes.map(n => buildJob(n, configOnly)).head

      job.configString = content
      println("-----------job: "+job.name)
      println("job.id: "+ job.id)
      println("job.pipeline: "+job.pipeline)
      Some(job)
    } else {
      None
    }
  }

  def buildByFile(fileName: String, configOnly: Boolean = false): Option[Job] = {
    println("filename is: "+fileName)
    val stream : InputStream = getClass.getResourceAsStream(fileName)
    println("get input stream success !")
    val lines = Source.fromInputStream(stream).getLines().mkString("\n")
    //    println(lines)
    buildByString(lines)
  }

  private def buildJob(fn: Node, configOnly: Boolean = false): Job = {
    println("start to build job !")
    val job = new Job
    job.name = (fn \ "@name").mkString.trim
    assert(job.name != null && job.name.length > 0, s"job.name must not be empty! Node = ${fn}")
    job.authors = (fn \ "@authors").mkString
    val configs = fn \ "config"
    println("start to handle configs")
    configs.foreach(cc => {
      val name = (cc \ "@name").mkString
      val value = (cc \ "@value").mkString
      job.addConfig(name, value)
    })

    println("is build pipeline: "+ configOnly.toString)
    if (!configOnly) {
      val pipelineNodes = fn \ "pipeline"
      println("start to build pipeline !")
      job.pipeline = pipelineNodes.map(n => buildPipeline(n, job)).head
      println("build pipeline over ")
    }
    job
  }

  private def buildWorkflow(fn: Node, job: Job): Workflow = {
    val id = (fn\ID_LITERAL).mkString
    val workflow = new Workflow(id, job)
    buildWorkflowNodes(fn, workflow)
    workflow
  }

  private def buildPipeline(fn: Node, job: Job): Pipeline = {
    println("start to build buildPipeline !")
    val id = (fn\ID_LITERAL).mkString
    println("id is: "+id)
    val pipeline = new Pipeline(id, job)
    println(pipeline)
    buildWorkflowNodes(fn, pipeline)
    println("buildWorkflowNodes over !!!" )
    println(pipeline)
    pipeline
  }

  /**
   * 相比于完整的 buildWorkflowNodes()，这里的 Simple 版本只做 pipeline 级别的解析，而不深入到 node 级别。
   */
  protected def buildWorkflowNodesSimple(fn: Node, workflow: Workflow): Unit = {
    processProperties(fn, workflow)
  }

  /**
   * 注意，修改此方法时，请酌情同样修改对应的 buildWorkflowNodesSimple() 方法。
   */
  def buildWorkflowNodes(fn: Node, workflow: Workflow): Unit = {
    val nodes = fn \ "node"
    println("node num: "+nodes.length.toString)
    processProperties(fn, workflow)
    println("handle properties over !")
    nodes.map(buildWorkflowNode).foreach(workflow.addChild)
    println("add workflow child over !")
    nodes.foreach(resolveDependency)
    println("load dependency"+ nodes.map(n=>n.label).mkString(","))
    workflow.init()

  }

  private def processProperties(nn: Node, c: Context): Unit = {
    val properties = nn \ "properties" \ "property"
    properties.foreach(nnn => {
      val key = (nnn \ "@key").mkString
      val value = (nnn \ "@value").mkString
      println(key+"->"+value)
      c.addVar(key, value)
    })
  }

  private def buildWorkflowNode(nn: Node): BaseNode = {
    val processorName = nn \ "@processor"
    println("processorName: "+processorName)

    val classPerson = mirror.staticClass(processorName.mkString)
    println("val classPerson = mirror.staticClass(processorName.mkString)")

    val cm = mirror.reflectClass(classPerson)
    println("val cm = mirror.reflectClass(classPerson)")
    // <init> 代表构造方法
    // val ctor = classPerson.toType.decl(TermName("<init>")).asMethod
    val ctor = classPerson.toType.declarations.find(m => m.isMethod && m.asMethod.isPrimaryConstructor).get.asMethod
    println("val ctor = classPerson.toType.declarations.find(m => m.isMethod && m.asMethod.isPrimaryConstructor).get.asMethod")
    val ctorm = cm.reflectConstructor(ctor)
    println("val ctorm = cm.reflectConstructor(ctor)")

    val fid = (nn \ ID_LITERAL).mkString
    println("ID_LITERAL: "+ID_LITERAL)

    val realObject = ctorm(fid)
    println("val realObject = ctorm(fid)")

    val n = realObject.asInstanceOf[BaseNode]
    println("val n = realObject.asInstanceOf[BaseNode]")

    nodeMap.put(fid, n)
    println("add node success !")

    processProperties(nn, n)
    println("handle each node properties over ")
    n
  }

  private def retrieveInputType(node: BaseNode): Type = {
    val classSymbol = mirror.reflect(node).symbol
    // scalastyle:off
    import compat._
    val parent = if (node.isInstanceOf[UnaryNode[_, _]]) {
      typeOf[UnaryNode[_, _]].typeSymbol.asClass
    } else if (node.isInstanceOf[MultiInputsNode[_, _]]) {
      typeOf[MultiInputsNode[_, _]].typeSymbol.asClass
    } else {
      throw new WorkflowDefineException(s"${node} is not right type")
    }
    val T = parent.typeParams(0).asType.toType
    T.asSeenFrom(ThisType(classSymbol), parent)
  }

  private def retrieveOutputType(node: BaseNode): Type = {
    val classSymbol = mirror.reflect(node).symbol
    // scalastyle:off
    import compat._
    val parent = if (node.isInstanceOf[UnaryNode[_, _]]) {
      typeOf[UnaryNode[_, _]].typeSymbol.asClass
    } else if (node.isInstanceOf[MultiInputsNode[_, _]]) {
      typeOf[MultiInputsNode[_, _]].typeSymbol.asClass
    } else if (node.isInstanceOf[GeneratorNode[_]]) {
      typeOf[GeneratorNode[_]].typeSymbol.asClass
    } else {
      throw new WorkflowDefineException(s"${node} is not right type")
    }
    val T = if (node.isInstanceOf[GeneratorNode[_]]) {
      parent.typeParams(0).asType.toType
    } else {
      parent.typeParams(1).asType.toType
    }
    T.asSeenFrom(ThisType(classSymbol), parent)
  }

  private def resolveDependency(nn: Node): Unit = {
    val id = (nn \ ID_LITERAL).mkString
    val cn = nodeMap.get(id)
    cn.isDefined match {
      case true => {
        val dependency = nn \ "dependency" \ "node"
        dependency.foreach(n => {
          val did = (n \ ID_LITERAL).mkString
          val dn = nodeMap.get(did)
          dn.isDefined match {
            case true => {
              cn.get.addInNode(dn.get)
              // 检查pipeline的输入输出类型是否匹配
              if (cn.get.isInstanceOf[UnaryNode[Any, Any]] || cn.get.isInstanceOf[MultiInputsNode[Any, Any]]) {
                val inputType = retrieveInputType(cn.get)
                val outputType = retrieveOutputType(dn.get)
                if (inputType != outputType) {
                  throw new WorkflowDefineException(s"(node = ${cn.get}: inputType = ${inputType}) does not match (node = ${dn.get}: outputType = ${outputType})")
                }
              }
              // 对于MultiInputsNode的多个输入进行命名区分
              if (cn.get.isInstanceOf[MultiInputsNode[Any, Any]]) {
                val input: String = (n \ INPUT_LITERAL).mkString
                if (!input.isEmpty) {
                  cn.get.asInstanceOf[MultiInputsNode[Any, Any]].inputConfigMap.put(input, dn.get)
                }
              }
            }
            case false => throw WorkflowDefineException(s"Node-${did} not created correctly")
          }
        })
      }
      case false => throw WorkflowDefineException(s"Node-${id} not created correctly")
    }
  }

}

/*
class SimpleXmlWorkflowBuilder extends XmlWorkflowBuilder {
  override def buildWorkflowNodes(fn: Node, workflow: Workflow): Unit = {
    buildWorkflowNodesSimple(fn, workflow)
  }
}

object XmlWorkflowBuilder {

  def main(args: Array[String]): Unit = {
    val builder = new XmlWorkflowBuilder
    val flow = builder.buildByFile("/list_example.xml")
    // scalastyle:off println
    println(flow)
  }
}

 */

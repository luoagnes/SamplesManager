package com.tencent.openrc.MSM.DF.transform

import com.tencent.openrc.MSM.CS.operation.Datahandler.{calc_bounary_and_index}
import com.tencent.openrc.MSM.DF.pipeline.node.UnaryNode
import com.tencent.openrc.MSM.DF.util.SparkRegistry
import org.slf4j.LoggerFactory
import com.tencent.openrc.MSM.CS.operation.Readparas.readWholeConfigure
import com.tencent.openrc.MSM.CS.util.IO.{printlog}

class CalcBoundaryIndexTansform(id: String) extends UnaryNode[Unit, Unit](id) {

    private val log = LoggerFactory.getLogger(this.getClass)
    private val ss = SparkRegistry.spark
    override def doExecute(input: Unit): Unit = {
        val currDate = getPropertyOrThrow("currDate")
        val configure_path = getPropertyOrThrow("configure_path")

        printlog("currDate: " + currDate)
        printlog("configure_path: " + configure_path)

        val configure = readWholeConfigure(ss, configure_path) // 读取整个configure
        printlog("feature config load over!")

        // handle_single_table

        if(configure.context_feature_conf_list.length > 0) {
            calc_bounary_and_index(
                ss,
                configure.action_input_conf,
                configure.context_feature_conf_list,
                1000,
                "equi_freq",
                configure.tdw_user,
                configure.tdw_passwd,
                configure.root_path
            )

        }

        for (data_conf <- configure.data_conf_list) {
            calc_bounary_and_index(
                ss,
                data_conf.input_conf,
                data_conf.feature_conf_list,
                1000,
                "equi_freq",
                configure.tdw_user,
                configure.tdw_passwd,
                configure.root_path
            )
        }
    }
}

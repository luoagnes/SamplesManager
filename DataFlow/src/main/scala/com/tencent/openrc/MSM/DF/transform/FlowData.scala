package com.tencent.openrc.MSM.DF.transform


import scala.beans.BeanProperty

class FlowData extends Serializable {
    @BeanProperty
    var partitionTime: String = _
    @BeanProperty
    var data: Array[Byte] = _
}



## 一. 介绍
本项目是一个多模块的项目，围绕着模型前的数据处理。目前包含三个模块：
CommonSection：集成了常见的数据处理和宽表拼接逻辑。宽表拼接的逻辑可以参考baopin_feature_join.xml
DataFlow：主要是针对样本整体做一个处理，包括离散化，样本划分，样本不均衡处理和采样等。统一入口是com.tencent.openrc.MSM.DF.driver.DataFlowApp.相关配置可以参考dataflow.xml

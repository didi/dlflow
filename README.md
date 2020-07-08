
[![contributions](https://img.shields.io/badge/contributions-welcome-brightgreen.svg)](CONTRIBUTING.md)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![python](https://img.shields.io/badge/python-%3E%3D3.6-blue.svg)]()


# DLFlow - A Deep Learning WorkFlow


## DLFlow概述
DLFlow是一套深度学习pipeline，它结合了Spark的大规模特征处理能力和Tensorflow模型构建能力。利用DLFlow可以快速处理原始特征、训练模型并进行大规模分布式预测，十分适合离线环境下的生产任务。利用DLFlow，用户只需专注于模型开发，而无需关心原始特征处理、pipeline构建、生产部署等工作。


## 功能支持
**配置驱动：** DLFlow通过配置驱动，修改配置可以快速更换特征、模型超参数、任务流程等等，极大提高工作效率。

**模块化结构：** 任务和模型以插件形式存在，便于使用和开发，用户可以可以轻地将自定义任务和模型注册到框架内使用。

**任务自组织：** 通过内置的Workflow框架，根据任务的产出标记自动解决任务依赖，轻松构建深度学习pipeline。

**最佳实践：** 融入滴滴用户画像团队深度学习离线任务的最佳实践，有效应对离线生产中的多种问题。将Tensorflow和Spark进行合理结合，更适合离线深度学习任务。

## 快速开始

### 环境准备
首先请确保环境中已经安装和配置Hadoop和Spark，并设置好了基本的环境变量。

- **Tensorflow访问HDFS**

为了能够使用让Tensorflow访问HDFS，需要确保如下环境变量生效：
```bash
# 确保libjvm.so被添加到LD_LIBRARY_PATH
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${JAVA_HOME}/jre/lib/amd64/server

# 确保hadoop jars被添加到CLASSPATH
export CLASSPATH=${CLASSPATH}:$(hadoop classpath --glob)
```
关于Tensorflow访问HDFS更多内容请参见 [TensorFlow on Hadoop](https://github.com/tensorflow/examples/blob/master/community/en/docs/deploy/hadoop.md)。

- **Spark读写TFReocrds**
```bash
# Clone tensorflow/ecosystem项目
git clone https://github.com/tensorflow/ecosystem.git

cd ecosystem/spark/spark-tensorflow-connector/

# 构建spark-tensorflow-connector
mvn versions:set -DnewVersion=1.14.0
mvn clean install
```
项目构建后生成 `target/spark-tensorflow-connector_2.11-1.14.0.jar`，后续需要确保该jar被添加到 
`spark.jars` 中。
关于Spark读写TFRecoreds更多内容请参见 [spark-tensorflow-connector](https://github.com/tensorflow/ecosystem/tree/master/spark/spark-tensorflow-connector)。



### 安装

通过pip安装：
```
pip install dlflow
```

通过源代码安装：
```
git clone  https://github.com/didi/dlflow.git
cd dlflow
python setup.py install
```

### 使用
- **配置文件**

运行配置可参考 `conf` 目录中的配置。
关于配置详情请参考 [配置说明](docs/tutorials/zh/CONFIGURATION.md)。

- **以模块运行**

```bash
python -m dlflow.main --config <CONFIGURATION FILE>.conf
```

- **以脚本运行**

确保python环境的 `bin` 目录已经被添加到环境变量 `PATH` 中
```bash
export PATH=$PATH:/usr/local/python/bin
```
之后通过如下命令运行
```
dlflow --config <CONFIGURATION FILE>.conf
```

更详细的使用参见 [使用说明](docs/tutorials/zh/USAGE.md)。

### 预定义任务
| 预定义任务 | 描述 |
| :---: | :--- |
| **Merge** | 特征融合任务，请参见 [特征融合](dmflow/README.md) |
| **Encode** | 解析原始特征，对特征进行编码和预处理，生成能够直接输入模型的特征 |
| **Train** | 模型训练任务 |
| **Evaluate** | 模型评估任务 |
| **Predict** | 模型预测任务，使用Spark进行分布式预测，具备处理大规模数据能力 |


## 手册目录
- [使用说明](docs/tutorials/zh/USAGE.md)
- [配置说明](docs/tutorials/zh/CONFIGURATION.md)
- [Fmap说明](docs/tutorials/zh/FMAP.md)
- [模型开发](docs/tutorials/zh/MODEL_DEV.md)
- [任务开发](docs/tutorials/zh/TASK_DEV.md)
- [版本记录](docs/tutorials/zh/RELEASE_NOTES.md)


## 技术方案
**DLFlow整体架构**

![整体架构](https://gitee.com/wubalabadubdub/dlflow/raw/master/architecture.png)

**DLFLow pipeline**

![Pipeline](https://gitee.com/wubalabadubdub/dlflow/raw/master/pipeline.png)


## Contributing
欢迎使用并参与到本项目的建设中，详细内容请参见 [Contribution Guide](CONTRIBUTING.md)。


## License
DLFlow 基于Apache-2.0协议进行分发和使用，更多信息参见 [LICENSE](LICENSE)。



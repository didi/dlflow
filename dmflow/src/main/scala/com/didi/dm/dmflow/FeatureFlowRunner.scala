package com.didi.dm.dmflow

import com.didi.dm.dmflow.common.hdfs.HdfsUtility
import com.didi.dm.dmflow.common.time.TimeUtils
import com.didi.dm.dmflow.feature.flow.{FeatureFlow, FeatureFlowModel}
import com.didi.dm.dmflow.spark.app.SparkAppBase
import com.google.common.base.Strings
import scopt.OptionParser

case class FeatureFlowRunnerParam(
                                   seedSQL: String = "",
                                   featureConfig: String = "",
                                   featureDate: String = null,
                                   featureModelHDFS: String = "",
                                   featureOutHDFS: String = "",
                                   fit: Boolean = false
                                 ) {

  def this() = this(seedSQL = "", featureConfig = "", featureDate = "", featureModelHDFS = "", featureOutHDFS = "", fit = false)

}

class FeatureFlowRunner {

}

object FeatureFlowRunner extends SparkAppBase[FeatureFlowRunnerParam] {

  override def run(param: FeatureFlowRunnerParam): Unit = {

    val seedDF = sparkSession.sql(param.seedSQL)
    require(seedDF.count() > 0, s"load seed dataframe from SQL is empty: ${param.seedSQL}")

    val execDate = TimeUtils.parseLocalDate(param.featureDate)

    if (param.fit) {
      require(!Strings.isNullOrEmpty(param.featureConfig), s"FeatureFlowRunner fit=true but required featureConfig is empty！${param.featureConfig}")

      logInfo(s"execDate=${param.featureDate}, begin to fit FeatureFlow Model by config: ${param.featureConfig}")
      val model = new FeatureFlow()
        .setConfigPath(param.featureConfig)
        .setExecuteDate(execDate)
        .setCheckpoint(param.featureOutHDFS)
        .fit(seedDF)

      logInfo(s"save FeatureFlow Model to HDFS: ${param.featureModelHDFS}")
      model.save(param.featureModelHDFS)
    } else {
      require(HdfsUtility.checkSuccess(param.featureModelHDFS + "/metadata"), s"load FeatureFlowModel but not exists: ${param.featureModelHDFS}")

      logInfo(s"execDate=${param.featureDate}, begin to merge feature by  FeatureFlowModel on HDFS: ${param.featureModelHDFS}")
      val model = FeatureFlowModel
        .load(param.featureModelHDFS)
        .setCheckpoint(param.featureOutHDFS)
        .setExecuteDate(execDate)
      model.transform(seedDF)
    }


  }

  override def getOptParser: OptionParser[FeatureFlowRunnerParam] = {
    val parser = new OptionParser[FeatureFlowRunnerParam]("test") {

      opt[String]("seedSQL")
        .required()
        .action((x, c) => c.copy(seedSQL = x))
        .validate { x =>
          if (!Strings.isNullOrEmpty(x)) success
          else failure(s"input seedSQL is null or empty")
        }

      opt[String]("featureConfig")
        .action((x, c) => c.copy(featureConfig = x))

      opt[String]("featureDate")
        .required()
        .action((x, c) => c.copy(featureDate = x))
        .validate { x =>
          if (!Strings.isNullOrEmpty(x)) success
          else failure(s"input featureDate is null or empty")
        }

      opt[String]("featureModelHDFS")
        .required()
        .action((x, c) => c.copy(featureModelHDFS = x))
        .validate { x =>
          if (!Strings.isNullOrEmpty(x)) success
          else failure(s"input featureModelHDFS is null or empty")
        }

      opt[String]("featureOutHDFS")
        .required()
        .action((x, c) => c.copy(featureOutHDFS = x))
        .validate { x =>
          if (!Strings.isNullOrEmpty(x)) success
          else failure(s"input featureOutHDFS is null or empty")
        }

      opt[Boolean]("fit")
        .action((x, c) => c.copy(fit = x))
    }
    parser
  }


}

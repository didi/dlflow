package com.didi.dm.dmflow.feature.flow

import com.didi.dm.dmflow.common.hdfs.HdfsUtility
import com.didi.dm.dmflow.common.io.IColorText
import com.didi.dm.dmflow.common.time.TimeUtils.Timer
import com.didi.dm.dmflow.config.Checkpoint
import com.didi.dm.dmflow.config.FeatureFlowConfig.GlobalSettings
import com.didi.dm.dmflow.spark.io.Checkpointable
import com.google.common.base.Strings
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame


abstract class BaseOperator(val uid: String,
                            @transient val meta: Step,
                            @transient val settings: GlobalSettings,
                            val ckpBaseHDFS: String
                           ) extends Checkpointable with IColorText with Identifiable {
  require(!Strings.isNullOrEmpty(ckpBaseHDFS), "input ckpBaseHDFS for BaseOperator is null or empty")

  @transient
  protected val ckpMeta: Checkpoint = getCheckpointByOrder
  if (ckpMeta.enable && ckpMeta.path.isDefined) setCheckpoint(ckpMeta.path.get)

  def getRuntimeCkp: Checkpoint = ckpMeta

  protected def process(inDF: DataFrame): DataFrame

  protected def getCheckpointByOrder: Checkpoint = {

    def hasCkpGlobal: Boolean = {
      settings.enableOverride && settings.checkpointConfig.enable
    }

    def hasCkpCustom: Boolean = {
      meta.checkpoint.isDefined && meta.checkpoint.get.enable
    }

    def hasCkpDefault: Boolean = {
      !settings.enableOverride && settings.checkpointConfig.enable
    }

    var defaultCkp = settings.checkpointConfig.copy()

    def fixBaseHDFS(input: Option[String], default: String): String = {
      require(!Strings.isNullOrEmpty(default.trim), "default checkpoint for fixBaseHDFS() is null or emtpy")
      input match {
        case Some(pt) => if (Strings.isNullOrEmpty(pt.trim)) default else pt
        case None => default
      }
    }

    var ckpHit = "default"
    if (hasCkpGlobal || hasCkpCustom || hasCkpDefault) {
      var ckpPath = ckpBaseHDFS
      if (hasCkpGlobal) { // global + overwrite
        ckpPath = fixBaseHDFS(settings.checkpointConfig.path, ckpPath) + "/" + uid
        defaultCkp = settings.checkpointConfig.copy(path = Some(ckpPath))
        ckpHit = "globalOverwrite"
      } else if (hasCkpCustom) { // custom
        ckpPath = fixBaseHDFS(meta.checkpoint.get.path, ckpPath) + "/" + uid
        defaultCkp = meta.checkpoint.get.copy(path = Some(ckpPath))
        ckpHit = "custom"
      } else if (hasCkpDefault) { // default
        ckpPath = fixBaseHDFS(settings.checkpointConfig.path, ckpPath) + "/" + uid
        defaultCkp = settings.checkpointConfig.copy(path = Some(ckpPath))
        ckpHit = "globalDefault"
      }
    } else {
      // close checkpoint
      defaultCkp = defaultCkp.copy(enable = false)
    }
    logDebug(s"""uid=$uid getCheckpointByOrder hit=$ckpHit using: ${defaultCkp.toString}""")
    defaultCkp
  }


  def runStep(inDF: DataFrame): DataFrame = {

    logInfo(Yellow("[STEP] ") + s"""begin to run FeatureFlow OP (uid: $uid, name: ${meta.name}, type: ${meta.opType.name})""")

    val timer = Timer().start()

    if (ckpMeta.enable && !ckpMeta.overwrite && HdfsUtility.checkSuccess(ckpMeta.path.get)) {
      logInfo(s"Using checkpoint and do NOT overwrite it, loading ckp from: ${ckpMeta.path.get}")
      return loadCheckpoint()
    }

    if (ckpMeta.overwrite && HdfsUtility.checkSuccess(ckpMeta.path.get)) {
      logWarning("checkpoint overwrite=true, delete Exists HDFS before running step")
      HdfsUtility.del(ckpMeta.path.get)
    }

    val transDF = process(inDF)

    val outDF = trySaveCheckpoint(transDF)
    log.info(s"run FeatureFlow OP finished! elasped: ${timer.end().toMinutes} mins")
    outDF
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

}

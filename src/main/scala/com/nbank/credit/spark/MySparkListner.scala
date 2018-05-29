package com.nbank.credit.spark

import org.apache.spark.scheduler._

class MySparkListner extends SparkListener {

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =  println("On Stage Completed " + stageCompleted.stageInfo.name + " Details" + stageCompleted.stageInfo.details)

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = println("On Stage Submitted " + stageSubmitted.stageInfo.name + " Details" + stageSubmitted.stageInfo.details)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = println("On Task End " + taskEnd.taskInfo.taskId)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = println("On Job Start " + jobStart.jobId + " " + jobStart.stageInfos)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = println("On Job end " + jobEnd.jobId + " " + jobEnd.jobResult)

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {}

  override def onOtherEvent(event: SparkListenerEvent): Unit = {}
}

/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.blaze.plan

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.blaze.Shims
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, BasicWriteTaskStatsTracker, WriteTaskStatsTracker}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.util.SerializableConfiguration

case class NativeParquetInsertIntoHiveTableExec(
    cmd: InsertIntoHiveTable,
    override val child: SparkPlan)
    extends NativeParquetInsertIntoHiveTableBase(cmd, child) {

  override protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable = new BlazeInsertIntoHiveTable(
    table,
    partition,
    query,
    overwrite,
    ifPartitionNotExists,
    outputColumnNames,
    metrics)

  override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
    copy(child = newChildren.head)
}

class BlazeInsertIntoHiveTable(
    table: CatalogTable,
    partition: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    outputColumnNames: Seq[String],
    outerMetrics: Map[String, SQLMetric])
    extends InsertIntoHiveTable(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists,
      outputColumnNames) {

  override lazy val metrics: Map[String, SQLMetric] = outerMetrics

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
    val nativeParquetSink =
      Shims.get.createNativeParquetSinkExec(sparkSession, table, partition, child, metrics)
    super.run(sparkSession, nativeParquetSink)
  }

  override def basicWriteJobStatsTracker(hadoopConf: Configuration): BasicWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    new BasicWriteJobStatsTracker(serializableHadoopConf, metrics) {
      override def newTaskInstance(): WriteTaskStatsTracker = {
        new BasicWriteTaskStatsTracker(serializableHadoopConf.value) {
          //override def newRow(_row: InternalRow): Unit = {}
          //override def getFinalStats(): WriteTaskStats = {
          //  val outputFileStat = ParquetSinkTaskContext.get.processedOutputFiles.remove()
          //  BasicWriteTaskStats(
          //    numPartitions = 1,
          //    numFiles = 1,
          //    numBytes = outputFileStat.numBytes,
          //    numRows = outputFileStat.numRows)
          //}
        }
      }
    }
  }
}

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
package org.apache.spark.sql.hive.execution.blaze.plan

import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.{UUID, HashMap => JHashMap}

import scala.collection.immutable.SortedMap
import scala.collection.JavaConverters._
import scala.collection.Seq

import org.apache.hadoop.fs.FileSystem
import org.apache.paimon.CoreOptions
import org.apache.paimon.catalog.CatalogContext
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.options.Options
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.RowDataToObjectArrayConverter
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.blaze.{JniBridge, MetricNode, NativeConverters, NativeHelper, NativeRDD, NativeSupports, Shims}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.types.{NullType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration
import org.blaze.{protobuf => pb}

case class NativeHiveTableScanBase(baseHiveScan: HiveTableScanExec)
    extends LeafExecNode
    with NativeSupports
    with Logging {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
      .toSeq :+
      ("predicate_evaluation_errors", SQLMetrics
        .createMetric(sparkContext, "Native.predicate_evaluation_errors")) :+
      ("row_groups_pruned", SQLMetrics
        .createMetric(sparkContext, "Native.row_groups_pruned")) :+
      ("bytes_scanned", SQLMetrics.createSizeMetric(sparkContext, "Native.bytes_scanned")) :+
      ("io_time", SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time")) :+
      ("io_time_getfs", SQLMetrics
        .createNanoTimingMetric(sparkContext, "Native.io_time_getfs")): _*)

  override val output: Seq[Attribute] = baseHiveScan.output
  override val outputPartitioning: Partitioning = baseHiveScan.outputPartitioning

  private val relation = baseHiveScan.relation
  private val partitionSchema = relation.tableMeta.partitionSchema

  private lazy val partitions = getFilePartitions()
  private lazy val fileSizes = partitions
    .flatMap(_.files)
    .groupBy(_.filePath)
    .mapValues(_.map(_.length).sum)
    .map(identity) // make this map serializable

  // should not include partition columns
  protected def nativeFileSchema: pb.Schema =
    NativeConverters.convertSchema(StructType(relation.tableMeta.dataSchema.map {
      case field if baseHiveScan.requestedAttributes.exists(_.name == field.name) =>
        field.copy(nullable = true)
      case field =>
        // avoid converting unsupported type in non-used fields
        StructField(field.name, NullType, nullable = true)
    }))

  protected def nativePartitionSchema: pb.Schema =
    NativeConverters.convertSchema(partitionSchema)

  protected def nativeFileGroups: FilePartition => pb.FileGroup = (partition: FilePartition) => {
    // list input file statuses
    val nativePartitionedFile = (file: PartitionedFile) => {
      val nativePartitionValues = partitionSchema.zipWithIndex.map { case (field, index) =>
        NativeConverters.convertValue(
          file.partitionValues.get(index, field.dataType),
          field.dataType)
      }
      pb.PartitionedFile
        .newBuilder()
        .setPath(s"${file.filePath}")
        .setSize(fileSizes(file.filePath))
        .addAllPartitionValues(nativePartitionValues.asJava)
        .setLastModifiedNs(0)
        .setRange(
          pb.FileRange
            .newBuilder()
            .setStart(file.start)
            .setEnd(file.start + file.length)
            .build())
        .build()
    }
    pb.FileGroup
      .newBuilder()
      .addAllFiles(partition.files.map(nativePartitionedFile).toList.asJava)
      .build()
  }

  // check whether native converting is supported
  nativeFileSchema
  nativePartitionSchema
  nativeFileGroups

  protected def putJniBridgeResource(
      resourceId: String,
      broadcastedHadoopConf: Broadcast[SerializableConfiguration]): Unit = {
    val sharedConf = broadcastedHadoopConf.value.value
    JniBridge.resourcesMap.put(
      resourceId,
      (location: String) => {
        val getFsTimeMetric = metrics("io_time_getfs")
        val currentTimeMillis = System.currentTimeMillis()
        val fs = NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
          override def run(): FileSystem = {
            FileSystem.get(new URI(location), sharedConf)
          }
        })
        getFsTimeMetric.add((System.currentTimeMillis() - currentTimeMillis) * 1000000)
        fs
      })
  }

  protected def broadcastedHadoopConf: Broadcast[SerializableConfiguration] = {
    val sparkSession = Shims.get.getSqlContext(baseHiveScan).sparkSession
    val hadoopConf =
      sparkSession.sessionState.newHadoopConfWithOptions(Map.empty)
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
  }

  override def doExecuteNative(): NativeRDD = {
    val nativeMetrics = MetricNode(
      metrics,
      Nil,
      Some({
        case ("bytes_scanned", v) =>
          val inputMetric = TaskContext.get.taskMetrics().inputMetrics
          inputMetric.incBytesRead(v)
        case ("output_rows", v) =>
          val inputMetric = TaskContext.get.taskMetrics().inputMetrics
          inputMetric.incRecordsRead(v)
        case _ =>
      }))
    // val nativePruningPredicateFilters = this.nativePruningPredicateFilters
    val nativeFileSchema = this.nativeFileSchema
    val nativeFileGroups = this.nativeFileGroups
    val nativePartitionSchema = this.nativePartitionSchema

    val projection = schema.map(field => relation.schema.fieldIndex(field.name))
    val broadcastedHadoopConf = this.broadcastedHadoopConf
    val numPartitions = partitions.length

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      Nil,
      rddShuffleReadFull = true,
      (partition, _context) => {
        val resourceId = s"NativeHiveTableScan:${UUID.randomUUID().toString}"
        putJniBridgeResource(resourceId, broadcastedHadoopConf)

        val nativeFileGroup = nativeFileGroups(partition.asInstanceOf[FilePartition])
        val nativeParquetScanConf = pb.FileScanExecConf
          .newBuilder()
          .setNumPartitions(numPartitions)
          .setPartitionIndex(partition.index)
          .setStatistics(pb.Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .setFileGroup(nativeFileGroup)
          .addAllProjection(projection.map(Integer.valueOf).asJava)
          .setPartitionSchema(nativePartitionSchema)
          .build()

        val nativeParquetScanExecBuilder = pb.ParquetScanExecNode
          .newBuilder()
          .setBaseConf(nativeParquetScanConf)
          .setFsResourceId(resourceId)
          .addAllPruningPredicates(new java.util.ArrayList()) // not support this filter

        pb.PhysicalPlanNode
          .newBuilder()
          .setParquetScan(nativeParquetScanExecBuilder.build())
          .build()
      },
      friendlyName = "NativeRDD.HiveScan")
  }

  override protected def doCanonicalize(): SparkPlan = baseHiveScan.canonicalized

  override val nodeName: String =
    s"NativeHiveTableScan ${relation.tableMeta.identifier.unquotedString}"

  override def simpleString(maxFields: Int): String =
    s"$nodeName (${baseHiveScan.simpleString(maxFields)})"

  private def loadTable(): FileStoreTable = {
    val catalogContext =
      CatalogContext.create(getOptions, SparkSession.active.sessionState.newHadoopConf())
    FileStoreTableFactory.create(catalogContext)
  }

  private def getOptions: Options = {
    val parameters = new JHashMap[String, String]()
    parameters.put(CoreOptions.PATH.key, relation.tableMeta.location.toString)
    Options.fromMap(parameters)
  }

  private def getFilePartitions(): Array[FilePartition] = {
    val currentTimeMillis = System.currentTimeMillis()
    val sparkSession = Shims.get.getSqlContext(baseHiveScan).sparkSession
    val table = loadTable()
    val splits =
      table.newScan().plan().splits().asScala.map(split => split.asInstanceOf[DataSplit])
    logInfo(s"Get paimon splits elapse: ${System.currentTimeMillis() - currentTimeMillis} ms")

    val splitPartitions = if (relation.isPartitioned && relation.prunedPartitions.nonEmpty) {
      val partitionPathAndValues =
        relation.prunedPartitions.get.map { catalogTablePartition =>
          (
            catalogTablePartition.spec.map { case (k, v) => s"$k=$v" }.mkString("/"),
            catalogTablePartition.toRow(
              partitionSchema,
              sparkSession.sessionState.conf.sessionLocalTimeZone))
        }.toMap
      val rowDataToObjectArrayConverter = new RowDataToObjectArrayConverter(
        table.schema().logicalPartitionType())
      val partitionKeys = table.schema().partitionKeys()
      // pruning paimon splits
      splits
        .map { split =>
          val values = rowDataToObjectArrayConverter.convert(split.partition())
          val partitionPath = values.zipWithIndex
            .map { case (v, i) => s"${partitionKeys.get(i)}=${v.toString}" }
            .mkString("/")
          (split, partitionPathAndValues.getOrElse(partitionPath, null))
        }
        .filter(_._2 != null)
        .map(partition => SplitPartition(partition._2, partition._1))
    } else {
      splits.map(SplitPartition(InternalRow.empty, _))
    }
    logInfo(s"Pruning splits from ${splits.length} to ${splitPartitions.length}")
    logInfo(
      s"Selected splits after partition pruning:\n\t ${splitPartitions.map(_.split.bucketPath()).mkString("\n\t")}")

    // TODO: support parquet and avro
    val isSplitable = true
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes = getMaxSplitBytes(sparkSession, splitPartitions)
    logInfo(
      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")
    val partitionedFiles = splitPartitions
      .flatMap { partition =>
        partition.split.dataFiles().asScala.flatMap { dataFileMeta =>
          val filePath = s"${partition.split.bucketPath()}/${dataFileMeta.fileName()}"
          splitFiles(dataFileMeta, filePath, isSplitable, maxSplitBytes, partition.values)
        }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes).toArray
  }

  // fork {@link PartitionedFileUtil#splitFiles}
  private def splitFiles(
      dataFileMeta: DataFileMeta,
      filePath: String,
      isSplitable: Boolean,
      maxSplitBytes: Long,
      partitionValues: InternalRow): Seq[PartitionedFile] = {
    if (isSplitable) {
      (0L until dataFileMeta.fileSize() by maxSplitBytes).map { offset =>
        val remaining = dataFileMeta.fileSize() - offset
        val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
        PartitionedFile(partitionValues, filePath, offset, size)
      }
    } else {
      Seq(PartitionedFile(partitionValues, filePath, 0, dataFileMeta.fileSize()))
    }
  }

  // fork {@link FilePartition#maxSplitBytes}
  private def getMaxSplitBytes(
      sparkSession: SparkSession,
      selectedPartitions: Seq[SplitPartition]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.sparkContext.defaultParallelism)
    val totalBytes = selectedPartitions
      .flatMap(_.split.dataFiles().asScala.map(_.fileSize() + openCostInBytes))
      .sum
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }

  private case class SplitPartition(values: InternalRow, split: DataSplit)
}

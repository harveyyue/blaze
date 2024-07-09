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
package org.apache.spark.sql.execution.blaze.shuffle

import org.apache.spark.{MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManager, ShuffleBlockFetcherIterator}

import java.io.InputStream

class BlazeBlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    startMapId: Option[Int] = None,
    endMapId: Option[Int] = None,
    shouldBatchFetch: Boolean = false)
    extends BlazeBlockStoreShuffleReaderBase[K, C](handle, context)
    with Logging {

  override def readBlocks(): Iterator[(BlockId, InputStream)] = {
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId,
      startMapId.getOrElse(0),
      endMapId.getOrElse(Int.MaxValue),
      startPartition,
      endPartition)

    new ShuffleBlockFetcherIterator(
      context,
      blockManager.blockStoreClient,
      blockManager,
      blocksByAddress,
      (_, inputStream) => inputStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
      SparkEnv.get.conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT),
      SparkEnv.get.conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
      readMetrics,
      fetchContinuousBlocksInBatch).toCompletionIterator
  }

  private def fetchContinuousBlocksInBatch: Boolean = {
    val conf = SparkEnv.get.conf
    val serializerRelocatable = dep.serializer.supportsRelocationOfSerializedObjects
    val compressed = conf.get(config.SHUFFLE_COMPRESS)
    val codecConcatenation = if (compressed) {
      CompressionCodec.supportsConcatenationOfSerializedStreams(
        CompressionCodec.createCodec(conf))
    } else {
      true
    }
    val useOldFetchProtocol = conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)

    val doBatchFetch = shouldBatchFetch && serializerRelocatable &&
      (!compressed || codecConcatenation) && !useOldFetchProtocol
    if (shouldBatchFetch && !doBatchFetch) {
      logDebug(
        "The feature tag of continuous shuffle block fetching is set to true, but " +
          "we can not enable the feature because other conditions are not satisfied. " +
          s"Shuffle compress: $compressed, serializer relocatable: $serializerRelocatable, " +
          s"codec concatenation: $codecConcatenation, use old shuffle fetch protocol: " +
          s"$useOldFetchProtocol.")
    }
    doBatchFetch
  }
}

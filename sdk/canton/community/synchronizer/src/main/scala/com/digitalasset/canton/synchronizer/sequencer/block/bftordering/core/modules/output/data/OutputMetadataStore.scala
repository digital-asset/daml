// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.db.DbOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.InMemoryOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

trait OutputMetadataStore[E <: Env[E]] extends AutoCloseable {

  import OutputMetadataStore.*

  def insertBlockIfMissing(metadata: OutputBlockMetadata)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]

  protected final def insertBlockIfMissingActionName(metadata: OutputBlockMetadata): String =
    s"insert output metadata for block number ${metadata.blockNumber} if missing"

  def insertEpochIfMissing(
      metadata: OutputEpochMetadata
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]

  protected final def insertEpochIfMissingActionName(metadata: OutputEpochMetadata): String =
    s"insert output metadata for epoch number ${metadata.epochNumber} if missing"

  def getEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputEpochMetadata]]

  protected final def getEpochMetadataActionName(epochNumber: EpochNumber): String =
    s"get output metadata for epoch number $epochNumber if missing"

  def getBlockFromInclusive(initial: BlockNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Seq[OutputBlockMetadata]]

  protected final def getFromInclusiveActionName(initial: BlockNumber): String =
    s"get output block metadata from block number $initial inclusive"

  def getLatestBlockAtOrBefore(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]]

  protected final def getLatestAtOrBeforeActionName(
      timestamp: CantonTimestamp
  ): String =
    s"get latest output block metadata at or before $timestamp"

  def getFirstBlockInEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]]

  protected final def getFirstInEpochActionName(epochNumber: EpochNumber): String =
    s"get first output block metadata in epoch $epochNumber"

  def getLastBlockInEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]]

  protected final def getLastInEpochActionName(epochNumber: EpochNumber): String =
    s"get last output block metadata in epoch $epochNumber"

  def getLastConsecutiveBlock(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]]

  protected final val lastConsecutiveActionName: String = "get last consecutive block metadata"
}

object OutputMetadataStore {

  final case class OutputBlockMetadata(
      epochNumber: EpochNumber,
      blockNumber: BlockNumber,
      blockBftTime: CantonTimestamp,
  )

  final case class OutputEpochMetadata(
      epochNumber: EpochNumber,
      couldAlterOrderingTopology: Boolean,
  )

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): OutputMetadataStore[PekkoEnv] =
    storage match {
      case _: MemoryStorage =>
        new InMemoryOutputMetadataStore
      case dbStorage: DbStorage =>
        new DbOutputMetadataStore(dbStorage, timeouts, loggerFactory)
    }
}

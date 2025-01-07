// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data.db.DbOutputBlockMetadataStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data.memory.InMemoryOutputBlockMetadataStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

import OutputBlockMetadataStore.OutputBlockMetadata

trait OutputBlockMetadataStore[E <: Env[E]] extends AutoCloseable {

  def insertIfMissing(metadata: OutputBlockMetadata)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]

  protected def insertIfMissingActionName(metadata: OutputBlockMetadata): String =
    s"insert output metadata for block number ${metadata.blockNumber} if missing"

  def getFromInclusive(initial: BlockNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Seq[OutputBlockMetadata]]

  protected def getFromInclusiveActionName(initial: BlockNumber): String =
    s"get output block metadata from block number $initial inclusive"

  def getLatestAtOrBefore(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]]

  protected def getLatestAtOrBeforeActionName(
      timestamp: CantonTimestamp
  ): String =
    s"get latest output block metadata at or before $timestamp"

  def getFirstInEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]]

  protected def getFirstInEpochActionName(epochNumber: EpochNumber): String =
    s"get first output block metadata in epoch $epochNumber"

  def getLastInEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]]

  protected def getLastInEpochActionName(epochNumber: EpochNumber): String =
    s"get last output block metadata in epoch $epochNumber"

  def getLastConsecutive(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[OutputBlockMetadata]]
  protected val lastConsecutiveActionName: String = "get last consecutive block metadata"

  def setPendingChangesInNextEpoch(
      block: BlockNumber,
      areTherePendingCantonTopologyChanges: Boolean,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit]
  protected val setPendingChangesInNextEpochActionName: String =
    "set pending changes in next epoch block metadata"
}

object OutputBlockMetadataStore {

  final case class OutputBlockMetadata(
      epochNumber: EpochNumber,
      blockNumber: BlockNumber,
      blockBftTime: CantonTimestamp,
      // TODO(#22205): Consider using a separate table for the following epoch-level info; in particular,
      //  the "pending topology changes" flag is not stable and may get updated before the block is output;
      //  this is currently not a problem for state transfer, but avoiding updates would make reasoning simpler.
      // Cumulative over all blocks in the epoch (restart support).
      epochCouldAlterSequencingTopology: Boolean,
      // It may be updated later if the topology needs to be fetched. May be true only for the last block in an epoch.
      pendingTopologyChangesInNextEpoch: Boolean = false,
  )

  def apply(
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): OutputBlockMetadataStore[PekkoEnv] =
    storage match {
      case _: MemoryStorage =>
        new InMemoryOutputBlockMetadataStore
      case dbStorage: DbStorage =>
        new DbOutputBlockMetadataStore(dbStorage, timeouts, loggerFactory)
    }
}

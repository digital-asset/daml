// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext

class DbOutputBlockMetadataStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends OutputBlockMetadataStore[PekkoEnv]
    with DbStore {

  import storage.api.*
  private val profile = storage.profile

  private implicit val readEpoch: GetResult[OutputBlockMetadata] =
    GetResult { r =>
      OutputBlockMetadata(
        EpochNumber(r.nextLong()),
        BlockNumber(r.nextLong()),
        CantonTimestamp.assertFromLong(r.nextLong()),
        r.nextBoolean(),
        r.nextBoolean(),
      )
    }

  override def insertIfMissing(metadata: OutputBlockMetadata)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = insertIfMissingActionName(metadata)
    val future = storage.performUnlessClosingF(name) {
      storage.update_(
        profile match {
          case _: Postgres =>
            sqlu"""insert into
                     ord_metadata_output_blocks(
                       epoch_number,
                       block_number,
                       bft_ts,
                       epoch_could_alter_sequencing_topology,
                       pending_topology_changes_in_next_epoch
                     )
                     values (
                       ${metadata.epochNumber},
                       ${metadata.blockNumber},
                       ${metadata.blockBftTime},
                       ${metadata.epochCouldAlterSequencingTopology},
                       ${metadata.pendingTopologyChangesInNextEpoch}
                     )
                     on conflict (
                       epoch_number,
                       block_number,
                       bft_ts,
                       epoch_could_alter_sequencing_topology,
                       pending_topology_changes_in_next_epoch
                     ) do nothing"""
          case _: H2 =>
            sqlu"""merge into
                     ord_metadata_output_blocks using dual on (
                       ord_metadata_output_blocks.epoch_number =
                         ${metadata.epochNumber}
                       and ord_metadata_output_blocks.block_number =
                         ${metadata.blockNumber}
                       and ord_metadata_output_blocks.bft_ts =
                         ${metadata.blockBftTime}
                       and ord_metadata_output_blocks.epoch_could_alter_sequencing_topology =
                         ${metadata.epochCouldAlterSequencingTopology}
                       and ord_metadata_output_blocks.pending_topology_changes_in_next_epoch =
                         ${metadata.pendingTopologyChangesInNextEpoch}
                     )
                     when not matched then
                       insert (
                         epoch_number,
                         block_number,
                         bft_ts,
                         epoch_could_alter_sequencing_topology,
                         pending_topology_changes_in_next_epoch
                       )
                       values (
                         ${metadata.epochNumber},
                         ${metadata.blockNumber},
                         ${metadata.blockBftTime},
                         ${metadata.epochCouldAlterSequencingTopology},
                         ${metadata.pendingTopologyChangesInNextEpoch}
                       )"""
        },
        functionFullName,
      )
    }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def getFromInclusive(
      initial: BlockNumber
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Seq[OutputBlockMetadata]] = {
    val name = getFromInclusiveActionName(initial)
    val future = storage.performUnlessClosingF(name) {
      storage
        .query(
          sql"""
          select
            epoch_number,
            block_number,
            bft_ts,
            epoch_could_alter_sequencing_topology,
            pending_topology_changes_in_next_epoch
          from ord_metadata_output_blocks
          where block_number >= $initial
          """
            .as[OutputBlockMetadata]
            .map { blocks =>
              // because we may insert blocks out of order, we need to
              // make sure to never return a sequence of blocks with a gap
              val blocksUntilFirstGap = blocks
                .sortBy(_.blockNumber)
                .zipWithIndex
                .takeWhile { case (block, index) =>
                  index + initial == block.blockNumber
                }
                .map(_._1)
              blocksUntilFirstGap
            }(DirectExecutionContext(logger)),
          functionFullName,
        )
    }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def getLatestAtOrBefore(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getLatestAtOrBeforeActionName(timestamp)
    val future = storage.performUnlessClosingF(name) {
      // TODO(#23143): Figure out if we can transfer less data.
      storage
        .query(
          sql"""
          select
            epoch_number,
            block_number,
            bft_ts,
            epoch_could_alter_sequencing_topology,
            pending_topology_changes_in_next_epoch
          from ord_metadata_output_blocks
          where bft_ts <= $timestamp
          order by block_number desc
          limit 1
          """.as[OutputBlockMetadata],
          functionFullName,
        )
        .map(_.headOption)
    }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def getFirstInEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getFirstInEpochActionName(epochNumber)
    getSingleOrdered(epochNumber, name, order = "asc")
  }

  override def getLastInEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getLastInEpochActionName(epochNumber)
    getSingleOrdered(epochNumber, name, order = "desc")
  }

  private def getSingleOrdered(epochNumber: EpochNumber, name: String, order: String)(implicit
      traceContext: TraceContext
  ) = {
    val future = storage.performUnlessClosingF(name) {
      storage
        .query(
          sql"""
          select
            epoch_number,
            block_number,
            bft_ts,
            epoch_could_alter_sequencing_topology,
            pending_topology_changes_in_next_epoch
          from ord_metadata_output_blocks
          where epoch_number = $epochNumber
          order by block_number #$order
          limit 1
          """.as[OutputBlockMetadata],
          functionFullName,
        )
        .map(_.headOption)
    }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def getLastConsecutive(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = lastConsecutiveActionName
    val future = storage.performUnlessClosingF(name) {
      storage
        .query(
          sql"""
          select
            t.epoch_number,
            t.block_number,
            t.bft_ts,
            t.epoch_could_alter_sequencing_topology,
            t.pending_topology_changes_in_next_epoch
          from (select *, row_number() over (order by block_number) as idx from ord_metadata_output_blocks) t
          where t.idx = t.block_number + 1
          order by t.block_number desc
          limit 1;
          """.as[OutputBlockMetadata],
          functionFullName,
        )
        .map(_.headOption)
    }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def setPendingChangesInNextEpoch(
      block: BlockNumber,
      areTherePendingCantonTopologyChanges: Boolean,
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = setPendingChangesInNextEpochActionName
    val future = storage.performUnlessClosingF(name) {
      storage.update_(
        sqlu"""
          update ord_metadata_output_blocks
          set pending_topology_changes_in_next_epoch = $areTherePendingCantonTopologyChanges
          where block_number = $block
          """,
        functionFullName,
      )
    }
    PekkoFutureUnlessShutdown(name, future)
  }
}

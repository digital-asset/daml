// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext

class DbOutputMetadataStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends OutputMetadataStore[PekkoEnv]
    with DbStore {

  import OutputMetadataStore.*

  import storage.api.*

  private val profile = storage.profile

  private implicit val readBlock: GetResult[OutputBlockMetadata] =
    GetResult { r =>
      OutputBlockMetadata(
        EpochNumber(r.nextLong()),
        BlockNumber(r.nextLong()),
        CantonTimestamp.assertFromLong(r.nextLong()),
      )
    }

  private implicit val readEpoch: GetResult[OutputEpochMetadata] =
    GetResult { r =>
      OutputEpochMetadata(
        EpochNumber(r.nextLong()),
        r.nextBoolean(),
      )
    }

  override def insertBlockIfMissing(metadata: OutputBlockMetadata)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = insertBlockIfMissingActionName(metadata)
    val future = () =>
      storage.performUnlessClosingUSF(name) {
        storage.update_(
          profile match {
            case _: Postgres =>
              sqlu"""insert into
                     ord_metadata_output_blocks(
                       epoch_number,
                       block_number,
                       bft_ts
                     )
                     values (
                       ${metadata.epochNumber},
                       ${metadata.blockNumber},
                       ${metadata.blockBftTime}
                     )
                     on conflict (block_number) do nothing"""
            case _: H2 =>
              sqlu"""merge into
                     ord_metadata_output_blocks using dual on (
                       ord_metadata_output_blocks.block_number =
                         ${metadata.blockNumber}
                     )
                     when not matched then
                       insert (
                         epoch_number,
                         block_number,
                         bft_ts
                       )
                       values (
                         ${metadata.epochNumber},
                         ${metadata.blockNumber},
                         ${metadata.blockBftTime}
                       )"""
          },
          functionFullName,
        )
      }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def getBlock(
      blockNumber: BlockNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getBlockMetadataActionName(blockNumber)
    val future = () =>
      storage.performUnlessClosingUSF(name) {
        storage
          .query(
            sql"""
          select
            epoch_number,
            block_number,
            bft_ts
          from ord_metadata_output_blocks
          where block_number = $blockNumber
          """
              .as[OutputBlockMetadata]
              .map(_.headOption),
            functionFullName,
          )
      }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def insertEpochIfMissing(metadata: OutputEpochMetadata)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = insertEpochIfMissingActionName(metadata)
    val future = () =>
      storage.performUnlessClosingUSF(name) {
        storage.update_(
          profile match {
            case _: Postgres =>
              sqlu"""insert into
                     ord_metadata_output_epochs(
                       epoch_number,
                       could_alter_ordering_topology
                     )
                     values (
                       ${metadata.epochNumber},
                       ${metadata.couldAlterOrderingTopology}
                     )
                     on conflict (epoch_number) do nothing"""
            case _: H2 =>
              sqlu"""merge into
                     ord_metadata_output_epochs using dual on (
                       ord_metadata_output_epochs.epoch_number =
                         ${metadata.epochNumber}
                     )
                     when not matched then
                       insert (
                         epoch_number,
                         could_alter_ordering_topology
                       )
                       values (
                         ${metadata.epochNumber},
                         ${metadata.couldAlterOrderingTopology}
                       )"""
          },
          functionFullName,
        )
      }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def getEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[OutputEpochMetadata]] = {
    val name = getEpochMetadataActionName(epochNumber)
    val future = () =>
      storage.performUnlessClosingUSF(name) {
        storage
          .query(
            sql"""
          select
            epoch_number,
            could_alter_ordering_topology
          from ord_metadata_output_epochs
          where epoch_number = $epochNumber
          """
              .as[OutputEpochMetadata]
              .map(_.headOption),
            functionFullName,
          )
      }
    PekkoFutureUnlessShutdown(name, future)
  }

  override def getBlockFromInclusive(
      initial: BlockNumber
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Seq[OutputBlockMetadata]] = {
    val name = getFromInclusiveActionName(initial)
    val future = () =>
      storage.performUnlessClosingUSF(name) {
        storage
          .query(
            sql"""
          select
            epoch_number,
            block_number,
            bft_ts
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

  override def getLatestBlockAtOrBefore(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getLatestAtOrBeforeActionName(timestamp)
    val future = () =>
      storage.performUnlessClosingUSF(name) {
        storage
          .query(
            sql"""
          select
            epoch_number,
            block_number,
            bft_ts
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

  override def getFirstBlockInEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getFirstInEpochActionName(epochNumber)
    getSingleBlock(epochNumber, name, order = "asc")
  }

  override def getLastBlockInEpoch(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getLastInEpochActionName(epochNumber)
    getSingleBlock(epochNumber, name, order = "desc")
  }

  private def getSingleBlock(epochNumber: EpochNumber, name: String, order: String)(implicit
      traceContext: TraceContext
  ) = {
    val future = () =>
      storage.performUnlessClosingUSF(name) {
        storage
          .query(
            sql"""
          select
            epoch_number,
            block_number,
            bft_ts
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

  override def getLastConsecutiveBlock(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = lastConsecutiveActionName
    val future = () =>
      storage.performUnlessClosingUSF(name) {
        storage
          .query(
            sql"""
          select
            t.epoch_number,
            t.block_number,
            t.bft_ts
          from (select *, row_number() over (order by block_number) as idx from ord_metadata_output_blocks) t
          where t.idx = t.block_number + 1
          order by t.block_number desc
          limit 1
          """.as[OutputBlockMetadata],
            functionFullName,
          )
          .map(_.headOption)
      }
    PekkoFutureUnlessShutdown(name, future)
  }
}

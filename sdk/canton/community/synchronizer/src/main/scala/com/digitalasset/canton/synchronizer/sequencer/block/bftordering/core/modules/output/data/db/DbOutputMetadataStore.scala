// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.DbAction.ReadOnly
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.DbStorage.dbEitherT
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistLeaderSelectionPolicyState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.ExecutionContext

import DbStorage.Implicits.*

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
  private val converters = storage.converters

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

  private implicit val readLowerBound: GetResult[OutputMetadataStore.LowerBound] =
    GetResult { r =>
      OutputMetadataStore.LowerBound(
        EpochNumber(r.nextLong()),
        BlockNumber(r.nextLong()),
      )
    }

  private implicit val readLeaderSelectionPolicy: GetResult[BlacklistLeaderSelectionPolicyState] =
    converters.getResultByteArray.andThen { bytes =>
      BlacklistLeaderSelectionPolicyState.fromTrustedByteArray((), bytes) match {
        case Left(error) =>
          throw new DbDeserializationException(
            s"Could not deserialize proto leader selection state: $error"
          )
        case Right(value) => value
      }
    }

  private implicit val setLeaderSelectionPolicy
      : SetParameter[BlacklistLeaderSelectionPolicyState] = { (ls, pp) =>
    val array = ls.toByteArray
    converters.setParameterByteArray(array, pp)
  }

  override def insertBlockIfMissing(metadata: OutputBlockMetadata)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = insertBlockIfMissingActionName(metadata)
    val query = profile match {
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
    }
    val future = () => storage.update_(query, functionFullName)
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def getBlock(
      blockNumber: BlockNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getBlockMetadataActionName(blockNumber)
    val query = sql"""
          select
            epoch_number,
            block_number,
            bft_ts
          from ord_metadata_output_blocks
          where block_number = $blockNumber
          """
      .as[OutputBlockMetadata]
      .map(_.headOption)
    val future = () => storage.query(query, functionFullName)
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def insertEpochIfMissing(metadata: OutputEpochMetadata)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val name = insertEpochIfMissingActionName(metadata)
    val query = profile match {
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
    }
    val future = () => storage.update_(query, functionFullName)
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def getEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[OutputEpochMetadata]] = {
    val name = getEpochMetadataActionName(epochNumber)
    val query = sql"""
          select
            epoch_number,
            could_alter_ordering_topology
          from ord_metadata_output_epochs
          where epoch_number = $epochNumber
          """
      .as[OutputEpochMetadata]
      .map(_.headOption)
    val future = () => storage.query(query, functionFullName)
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def getBlockFromInclusive(
      initial: BlockNumber
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Seq[OutputBlockMetadata]] = {
    val name = getFromInclusiveActionName(initial)
    val query = sql"""
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
      }(DirectExecutionContext(logger))
    val future = () => storage.query(query, functionFullName)
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def getLatestBlockAtOrBefore(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = getLatestAtOrBeforeActionName(timestamp)
    val query = sql"""
          select
            epoch_number,
            block_number,
            bft_ts
          from ord_metadata_output_blocks
          where bft_ts <= $timestamp
          order by block_number desc
          limit 1
          """.as[OutputBlockMetadata]
    val future = () => storage.query(query, functionFullName).map(_.headOption)
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
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
  ): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val future = () =>
      storage.query(
        getSingleBlockDBIO(epochNumber, order),
        functionFullName,
      )
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  private def getSingleBlockDBIO(
      epochNumber: EpochNumber,
      order: String,
  ): ReadOnly[Option[OutputBlockMetadata]] =
    sql"""
          select
            epoch_number,
            block_number,
            bft_ts
          from ord_metadata_output_blocks
          where epoch_number = $epochNumber
          order by block_number #$order
          limit 1
          """.as[OutputBlockMetadata].headOption

  override def getLastConsecutiveBlock(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[OutputBlockMetadata]] = {
    val name = lastConsecutiveActionName
    val query = for {
      initialBlock <-
        sql"select block_number from ord_output_lower_bound"
          .as[Long]
          .headOption
          .map(_.fold(BlockNumber.First)(BlockNumber(_)))
      result <- sql"""
                select
                  t.epoch_number,
                  t.block_number,
                  t.bft_ts
                from (select *, row_number() over (order by block_number) as idx from ord_metadata_output_blocks) t
                where t.idx + $initialBlock = t.block_number + 1
                order by t.block_number desc
                limit 1
                """.as[OutputBlockMetadata].headOption
    } yield result
    val future = () => storage.query(query, functionFullName)
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def loadNumberOfRecords(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[NumberOfRecords] =
    PekkoFutureUnlessShutdown(
      loadNumberOfRecordsName,
      () =>
        storage.query(
          (for {
            numberOfEpochs <- sql"""select count(*) from ord_metadata_output_epochs""".as[Long].head
            numberOfBlocks <- sql"""select count(*) from ord_metadata_output_blocks""".as[Long].head
          } yield NumberOfRecords(numberOfEpochs, numberOfBlocks)),
          functionFullName,
        ),
      orderingStage = Some(functionFullName),
    )

  override def prune(epochNumberExclusive: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[NumberOfRecords] =
    PekkoFutureUnlessShutdown(
      pruneName(epochNumberExclusive),
      () =>
        for {
          epochsDeleted <- storage.update(
            sqlu""" delete from ord_metadata_output_epochs where epoch_number < $epochNumberExclusive """,
            functionFullName,
          )
          blocksDeleted <- storage.update(
            sqlu""" delete from ord_metadata_output_blocks where epoch_number < $epochNumberExclusive """,
            functionFullName,
          )
          _ <- storage.update(
            sqlu""" delete from ord_leader_selection_state where epoch_number < $epochNumberExclusive""",
            functionFullName,
          )
        } yield NumberOfRecords(
          epochsDeleted.toLong,
          blocksDeleted.toLong,
        ),
      orderingStage = Some(functionFullName),
    )

  override def getLowerBound()(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[LowerBound]] =
    PekkoFutureUnlessShutdown(
      getLowerBoundActionName,
      () =>
        storage.query(
          sql"select epoch_number, block_number from ord_output_lower_bound"
            .as[LowerBound]
            .headOption,
          functionFullName,
        ),
      orderingStage = Some(functionFullName),
    )

  override def saveLowerBound(epoch: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Either[String, Unit]] = PekkoFutureUnlessShutdown(
    saveLowerBoundName(epoch),
    () =>
      storage.queryAndUpdate(
        ((for {
          existingLowerBoundEpoch <- dbEitherT[String](
            sql"select epoch_number from ord_output_lower_bound".as[Long].headOption
          ).map(_.map(EpochNumber(_)))
          _ <- EitherT.fromEither[DBIO](
            existingLowerBoundEpoch
              .filter(_ > epoch)
              .map(existing => s"Cannot save lower bound $epoch earlier than existing $existing")
              .toLeft(())
          )
          block <-
            dbEitherT(
              getSingleBlockDBIO(epoch, "asc")
                .map {
                  case None =>
                    Left(
                      s"Cannot save lower bound at epoch $epoch because there are no blocks saved at this epoch"
                    )
                  case Some(blockNumber) => Right(blockNumber)
                }
            )
          _ <- dbEitherT[String](
            existingLowerBoundEpoch.fold(
              sqlu"insert into ord_output_lower_bound (epoch_number, block_number) values ($epoch, ${block.blockNumber})"
            )(_ =>
              sqlu"update ord_output_lower_bound set epoch_number = $epoch, block_number = ${block.blockNumber}"
            )
          )
        } yield ()).value),
        functionFullName,
      ),
    orderingStage = Some(functionFullName),
  )

  override def insertLeaderSelectionPolicyState(
      epochNumber: EpochNumber,
      leaderSelectionPolicyState: BlacklistLeaderSelectionPolicyState,
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] = {
    val name = insertLeaderSelectionPolicyStateActionName(epochNumber)
    val future = () => {
      storage.update_(
        profile match {
          case _: Postgres =>
            sqlu"""insert into
                     ord_leader_selection_state(
                       epoch_number,
                       state
                     )
                     values (
                       $epochNumber,
                       $leaderSelectionPolicyState
                     )
                     on conflict (epoch_number) do nothing"""
          case _: H2 =>
            sqlu"""merge into
                     ord_leader_selection_state using dual on (
                       ord_leader_selection_state.epoch_number =
                         $epochNumber
                     )
                     when not matched then
                       insert (
                         epoch_number,
                         state
                       )
                       values (
                         $epochNumber,
                         $leaderSelectionPolicyState
                       )"""
        },
        functionFullName,
      )
    }
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  override def getLeaderSelectionPolicyState(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Option[BlacklistLeaderSelectionPolicyState]] = {
    val name = getLeaderSelectionPolicyStateActionName(epochNumber)
    val future = () => {
      storage
        .query(
          sql"""
          select
            state
          from ord_leader_selection_state
          where epoch_number = $epochNumber
          """
            .as[BlacklistLeaderSelectionPolicyState]
            .map(_.headOption),
          functionFullName,
        )
    }
    PekkoFutureUnlessShutdown(name, future, orderingStage = Some(functionFullName))
  }

  def saveOnboardedNodeLowerBound(epoch: EpochNumber, blockNumber: BlockNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Either[String, Unit]] = {
    val initialLowerBound = LowerBound(epoch, blockNumber)
    val insertEitherT: EitherT[DBIO, String, Unit] = dbEitherT[String](
      sqlu"insert into ord_output_lower_bound (epoch_number, block_number) values ($epoch, $blockNumber)"
    ).map(_ => ())
    PekkoFutureUnlessShutdown(
      saveOnboardedNodeLowerBoundName(epoch, blockNumber),
      () =>
        storage
          .queryAndUpdate(
            (for {
              existingLowerBoundEpoch <- dbEitherT[String](
                sql"select epoch_number, block_number from ord_output_lower_bound"
                  .as[LowerBound]
                  .headOption
              )
              _ <- existingLowerBoundEpoch.fold(insertEitherT) { existing =>
                if (existing == initialLowerBound) EitherT.rightT(())
                else
                  EitherT.leftT(
                    s"The initial lower bound for this node has already been set to $existing, so cannot set it to $initialLowerBound"
                  )
              }
            } yield ()).value,
            functionFullName,
          ),
      orderingStage = Some(functionFullName),
    )
  }
}

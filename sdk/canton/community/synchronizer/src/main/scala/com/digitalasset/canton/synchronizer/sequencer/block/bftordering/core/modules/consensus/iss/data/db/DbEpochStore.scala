// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.db

import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterByteString
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.{
  Block,
  Epoch,
  EpochInProgress,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  EpochStoreReader,
  Genesis,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftNetworkMessage,
  PbftNormalCaseMessage,
  PbftViewChangeMessage,
  PrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.ConsensusMessage as ProtoConsensusMessage
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{BatchAggregator, FutureUnlessShutdownUtil}
import com.digitalasset.canton.{ProtoDeserializationError, RichGeneratedMessage}
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedResult, SetParameter}

import scala.concurrent.ExecutionContext
import scala.util.Try

import DbEpochStore.*

class DbEpochStore(
    batchAggregatorConfig: BatchAggregatorConfig,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends EpochStore[PekkoEnv]
    with EpochStoreReader[PekkoEnv]
    with DbStore {

  import storage.api.*

  private val profile = storage.profile

  private implicit val readEpoch: GetResult[EpochInfo] = GetResult { r =>
    EpochInfo(
      EpochNumber(r.nextLong()),
      BlockNumber(r.nextLong()),
      EpochLength(r.nextLong()),
      TopologyActivationTime(CantonTimestamp.assertFromLong(r.nextLong())),
    )
  }

  private implicit val readPbftMessage: GetResult[SignedMessage[PbftNetworkMessage]] =
    GetResult(
      parseSignedMessage(from =>
        // actual sender is not needed when reading from the store
        IssConsensusModule.parseConsensusNetworkMessage(from, actualSender = None, _)
      )
    )

  private implicit val tryReadPrePrepareMessageAndEpochInfo: GetResult[(PrePrepare, EpochInfo)] =
    GetResult { r =>
      // actual sender is not needed when reading from the store
      val prePrepare =
        parseSignedMessage(_ => PrePrepare.fromProtoConsensusMessage(actualSender = None, _))(r)
      prePrepare.message -> readEpoch(r)
    }

  private implicit val readCommitMessage: GetResult[SignedMessage[Commit]] = GetResult {
    // actual sender is not needed when reading from the store
    parseSignedMessage(_ => Commit.fromProtoConsensusMessage(actualSender = None, _))
  }

  // TODO(#28200): introduce `BatchAggregator#runTogether` that avoids splitting items into different batches
  //  and use it in `addPrepares` and `addOrderedBlock`

  private val insertInProgressPbftMessagesBatchAggregator =
    BatchAggregator(
      new InsertBatchAggregatorProcessor(
        { (seq, traceContext) =>
          implicit val tc: TraceContext = traceContext
          runInsertInProgressPbftMessages(seq)
        },
        "In-progress consensus block network message insert",
        logger,
      ),
      batchAggregatorConfig,
    )

  private def createFuture[X](
      actionName: String,
      orderingStage: String,
  )(future: => FutureUnlessShutdown[X])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[X] =
    PekkoFutureUnlessShutdown(
      actionName,
      () => storage.synchronizeWithClosing(actionName)(future),
      Some(orderingStage),
    )

  private def parseSignedMessage[A <: PbftNetworkMessage](
      parse: BftNodeId => ProtoConsensusMessage => ByteString => ParsingResult[A]
  )(
      r: PositionedResult
  ): SignedMessage[A] = {
    val messageBytes = r.nextBytes()

    val messageOrError = for {
      signedMessageProto <- Try(v30.SignedMessage.parseFrom(messageBytes)).toEither
        .leftMap(x => ProtoDeserializationError.OtherError(x.toString))
      message <- SignedMessage.fromProtoWithNodeId(v30.ConsensusMessage)(parse)(
        signedMessageProto
      )
    } yield message

    messageOrError.fold(
      error => throw new DbDeserializationException(s"Could not deserialize pbft message: $error"),
      identity,
    )
  }

  private implicit val setPbftMessagesParameter: SetParameter[SignedMessage[PbftNetworkMessage]] =
    (msg, pp) => pp >> msg.toProtoV1.checkedToByteString

  override def startEpoch(
      epoch: EpochInfo
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(startEpochActionName(epoch), orderingStage = functionFullName) {
      storage.update_(
        profile match {
          case _: Postgres =>
            sqlu"""insert into ord_epochs(epoch_number, start_block_number, epoch_length, topology_ts, in_progress)
                   values (${epoch.number}, ${epoch.startBlockNumber}, ${epoch.length}, ${epoch.topologyActivationTime.value}, true)
                   on conflict (epoch_number) do nothing
                """
          case _: H2 =>
            sqlu"""merge into ord_epochs
                   using dual on (
                     ord_epochs.epoch_number = ${epoch.number}
                   )
                   when not matched then
                     insert (epoch_number, start_block_number, epoch_length, topology_ts, in_progress)
                     values (${epoch.number}, ${epoch.startBlockNumber}, ${epoch.length}, ${epoch.topologyActivationTime.value},  true)
                """
        },
        functionFullName,
      )
    }

  override def completeEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(completeEpochActionName(epochNumber), orderingStage = functionFullName) {
      // asynchronously delete all in-progress messages after an epoch ends
      FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
        storage
          .update_(
            sqlu"""delete from ord_pbft_messages_in_progress where epoch_number <= $epochNumber""",
            functionFullName,
          ),
        failureMessage = "could not delete in-progress pbft messages from previous epoch(s)",
      )
      // synchronously update the completed epoch to no longer be in progress
      storage.update_(
        sqlu"""update ord_epochs set in_progress = false where epoch_number = $epochNumber""",
        functionFullName,
      )
    }

  override def latestEpoch(includeInProgress: Boolean)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Epoch] =
    createFuture(latestEpochActionName, orderingStage = functionFullName) {
      storage
        .query(
          for {
            epochInfo <-
              if (!includeInProgress)
                sql"""select epoch_number, start_block_number, epoch_length, topology_ts
                    from ord_epochs
                    where in_progress = false
                    order by epoch_number desc
                    limit 1
                 """.as[EpochInfo]
              else
                sql"""select epoch_number, start_block_number, epoch_length, topology_ts
                    from ord_epochs
                    order by epoch_number desc
                    limit 1
                 """.as[EpochInfo]
            epoch = epochInfo.lastOption.getOrElse(Genesis.GenesisEpochInfo)
            lastBlockCommitMessages <-
              sql"""select message
                  from ord_pbft_messages_completed pbft_message
                  where pbft_message.block_number = ${epoch.lastBlockNumber} and pbft_message.discriminator = $CommitMessageDiscriminator
                  order by pbft_message.from_sequencer_id
               """.as[SignedMessage[Commit]]
          } yield Epoch(epoch, lastBlockCommitMessages),
          functionFullName,
        )
    }

  override def addPrePrepare(prePrepare: SignedMessage[ConsensusMessage.PrePrepare])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] =
    createFuture(addPrePrepareActionName(prePrepare), orderingStage = functionFullName) {
      insertInProgressPbftMessagesBatchAggregator.run(prePrepare)
    }

  override def addPreparesAtomically(
      prepares: Seq[SignedMessage[ConsensusMessage.Prepare]]
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(addPreparesActionName, orderingStage = functionFullName) {
      // Cannot use the batch aggregator here as we need to make sure for CFT that all messages end up
      //  in the same transaction.
      runInsertInProgressPbftMessages(prepares)
    }

  override def addViewChangeMessage[M <: PbftViewChangeMessage](
      viewChangeMessage: SignedMessage[M]
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] =
    createFuture(
      addViewChangeMessageActionName(viewChangeMessage),
      orderingStage = functionFullName,
    ) {
      insertInProgressPbftMessagesBatchAggregator.run(viewChangeMessage)
    }

  override def addOrderedBlockAtomically(
      prePrepare: SignedMessage[PrePrepare],
      commitMessages: Seq[SignedMessage[Commit]],
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val epochNumber = prePrepare.message.blockMetadata.epochNumber
    val blockNumber = prePrepare.message.blockMetadata.blockNumber
    createFuture(
      addOrderedBlockActionName(epochNumber, blockNumber),
      orderingStage = functionFullName,
    ) {
      val messages: Seq[SignedMessage[PbftNormalCaseMessage]] =
        commitMessages :++ Seq[SignedMessage[PbftNormalCaseMessage]](prePrepare)
      // Cannot use the batch aggregator here as we need to make sure for CFT that all messages end up
      //  in the same transaction.
      runInsertFinalPbftMessages(messages)
    }
  }

  private def runInsertInProgressPbftMessages(
      messages: Seq[SignedMessage[PbftNetworkMessage]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val insertSql =
      profile match {
        case _: Postgres =>
          """insert into ord_pbft_messages_in_progress(block_number, epoch_number, view_number, message, discriminator, from_sequencer_id)
                   values (?, ?, ?, ?, ?, ?)
                   on conflict (block_number, view_number, discriminator, from_sequencer_id) do nothing
                """
        case _: H2 =>
          """merge into ord_pbft_messages_in_progress
                   using dual on (ord_pbft_messages_in_progress.block_number = ?1
                     and ord_pbft_messages_in_progress.epoch_number = ?2
                     and ord_pbft_messages_in_progress.view_number = ?3
                     and ord_pbft_messages_in_progress.discriminator = ?5
                     and ord_pbft_messages_in_progress.from_sequencer_id = ?6
                   )
                   when not matched then
                     insert (block_number, epoch_number, view_number, message, discriminator, from_sequencer_id)
                     values (?1, ?2, ?3, ?4, ?5, ?6)
                """
      }

    storage
      .runWrite(
        // Sorting should prevent deadlocks in Postgres when using concurrent clashing batched inserts
        //  with idempotency "on conflict do nothing" clauses.
        DbStorage
          .bulkOperation_(insertSql, messages.sortBy(key), storage.profile) { pp => msg =>
            pp >> msg.message.blockMetadata.blockNumber
            pp >> msg.message.blockMetadata.epochNumber
            pp >> msg.message.viewNumber
            pp >> msg
            pp >> getDiscriminator(msg.message)
            pp >> msg.from
          },
        functionFullName,
        maxRetries = 1,
      )
      .map(_ => ())
  }

  private def runInsertFinalPbftMessages[M <: PbftNetworkMessage](
      messages: Seq[SignedMessage[M]]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] = {
    val insertSql =
      profile match {
        case _: Postgres =>
          """insert into ord_pbft_messages_completed(block_number, epoch_number, message, discriminator, from_sequencer_id)
                 values (?, ?, ?, ?, ?)
                 on conflict (block_number, epoch_number, discriminator, from_sequencer_id) do nothing
              """
        case _: H2 =>
          """merge into ord_pbft_messages_completed
                 using dual on (ord_pbft_messages_completed.block_number = ?1
                   and ord_pbft_messages_completed.epoch_number = ?2
                   and ord_pbft_messages_completed.discriminator = ?4
                   and ord_pbft_messages_completed.from_sequencer_id = ?5
                 )
                 when not matched then
                   insert (block_number, epoch_number, message, discriminator, from_sequencer_id)
                   values (?1, ?2, ?3, ?4, ?5)
              """
      }
    storage
      .runWrite(
        // Sorting should prevent deadlocks in Postgres when using concurrent clashing batched inserts
        //  with idempotency "on conflict do nothing" clauses.
        DbStorage
          .bulkOperation_(insertSql, messages.sortBy(key), storage.profile) { pp => msg =>
            pp >> msg.message.blockMetadata.blockNumber
            pp >> msg.message.blockMetadata.epochNumber
            pp >> msg
            pp >> getDiscriminator(msg.message)
            pp >> msg.from
          },
        functionFullName,
        maxRetries = 1,
      )
      .map(_ => ())
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def loadEpochProgress(activeEpochInfo: EpochInfo)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[EpochInProgress] =
    createFuture(loadEpochProgressActionName(activeEpochInfo), orderingStage = functionFullName) {
      storage
        .query(
          for {
            completeBlockMessages <-
              sql"""select message
                    from ord_pbft_messages_completed
                    where epoch_number = ${activeEpochInfo.number}
                    order by from_sequencer_id, block_number
                 """.as[SignedMessage[PbftNetworkMessage]](readPbftMessage)
            pbftMessagesForIncompleteBlocks <-
              sql"""select message
                    from ord_pbft_messages_in_progress
                    where epoch_number = ${activeEpochInfo.number}
                    order by from_sequencer_id, block_number, discriminator, view_number
                 """
                // had to set the GetResult explicitly because for some reason it was picking the one for commit messages
                .as[SignedMessage[PbftNetworkMessage]](readPbftMessage)
          } yield {
            val commits = getCommits(completeBlockMessages)
            val sortedBlocks = completeBlockMessages
              .collect { case s @ SignedMessage(pp: PrePrepare, _) =>
                val blockNumber = pp.blockMetadata.blockNumber
                Block(
                  activeEpochInfo.number,
                  blockNumber,
                  CommitCertificate(
                    s.asInstanceOf[SignedMessage[PrePrepare]],
                    commits.getOrElse(blockNumber, Seq.empty),
                  ),
                )
              }
              .sortBy(_.blockNumber)
            EpochInProgress(sortedBlocks, pbftMessagesForIncompleteBlocks)
          },
          functionFullName,
        )
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def loadCompleteBlocks(
      startEpochNumberInclusive: EpochNumber,
      endEpochNumberInclusive: EpochNumber,
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Seq[Block]] =
    createFuture(
      loadPrePreparesActionName(startEpochNumberInclusive, endEpochNumberInclusive),
      orderingStage = functionFullName,
    ) {
      storage
        .query(
          for {
            completeBlockMessages <-
              sql"""select message
                    from ord_pbft_messages_completed
                    where epoch_number >= $startEpochNumberInclusive
                      and epoch_number <= $endEpochNumberInclusive
                    order by from_sequencer_id, block_number
                 """.as[SignedMessage[PbftNetworkMessage]](readPbftMessage)
          } yield {
            val commits = getCommits(completeBlockMessages)
            completeBlockMessages
              .collect { case s @ SignedMessage(pp: PrePrepare, _) =>
                val blockNumber = pp.blockMetadata.blockNumber
                Block(
                  pp.blockMetadata.epochNumber,
                  blockNumber,
                  CommitCertificate(
                    s.asInstanceOf[SignedMessage[PrePrepare]],
                    commits.getOrElse(blockNumber, Seq.empty),
                  ),
                )
              }
              .sortBy(_.blockNumber)
          },
          functionFullName,
        )
    }

  override def loadEpochInfo(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[EpochInfo]] =
    createFuture(loadEpochInfoActionName(epochNumber), orderingStage = functionFullName) {
      storage
        .query(
          sql"""select epoch_number, start_block_number, epoch_length, topology_ts
                from ord_epochs
                where epoch_number = $epochNumber
                limit 1
           """.as[EpochInfo],
          functionFullName,
        )
    }.map(_.headOption)

  override def loadOrderedBlocks(
      initialBlockNumber: BlockNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Seq[OrderedBlockForOutput]] =
    createFuture(
      loadOrderedBlocksActionName(initialBlockNumber),
      orderingStage = functionFullName,
    ) {
      storage
        .query(
          sql"""select
                  completed_message.message,
                  epoch.epoch_number, epoch.start_block_number, epoch.epoch_length, epoch.topology_ts
                from
                  ord_pbft_messages_completed completed_message
                  inner join ord_epochs epoch
                    on epoch.epoch_number = completed_message.epoch_number
                where
                  completed_message.discriminator = $PrePrepareMessageDiscriminator and
                  completed_message.block_number >= $initialBlockNumber
                order by
                  completed_message.block_number
              """.as[(PrePrepare, EpochInfo)](tryReadPrePrepareMessageAndEpochInfo),
          functionFullName,
        )
        .map {
          _.map { case (prePrepare, epochInfo) =>
            OrderedBlockForOutput(
              OrderedBlock(
                prePrepare.blockMetadata,
                prePrepare.block.proofs,
                prePrepare.canonicalCommitSet,
              ),
              prePrepare.viewNumber,
              prePrepare.from,
              epochInfo.lastBlockNumber == prePrepare.blockMetadata.blockNumber,
              OrderedBlockForOutput.Mode.FromConsensus,
            )
          }
        }
    }

  override def loadNumberOfRecords(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[EpochStore.NumberOfRecords] =
    createFuture(loadNumberOfRecordsName, orderingStage = functionFullName) {
      storage.query(
        for {
          numberOfEpochs <- sql"""select count(*) from ord_epochs""".as[Long].head
          numberOfMsgsCompleted <- sql"""select count(*) from ord_pbft_messages_completed"""
            .as[Long]
            .head
          numberOfMsgsInProgress <- sql"""select count(*) from ord_pbft_messages_in_progress"""
            .as[Int]
            .head
        } yield EpochStore
          .NumberOfRecords(numberOfEpochs, numberOfMsgsCompleted, numberOfMsgsInProgress),
        functionFullName,
      )
    }

  override def prune(epochNumberExclusive: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[EpochStore.NumberOfRecords] =
    createFuture(pruneName(epochNumberExclusive), orderingStage = functionFullName) {
      for {
        epochsDeleted <- storage.update(
          sqlu""" delete from ord_epochs where epoch_number < $epochNumberExclusive """,
          functionFullName,
        )
        pbftMessagesCompletedDeleted <- storage.update(
          sqlu""" delete from ord_pbft_messages_completed where epoch_number < $epochNumberExclusive """,
          functionFullName,
        )
        pbftMessagesInProgressDeleted <- storage.update(
          sqlu""" delete from ord_pbft_messages_in_progress where epoch_number < $epochNumberExclusive """,
          functionFullName,
        )
      } yield EpochStore.NumberOfRecords(
        epochsDeleted.toLong,
        pbftMessagesCompletedDeleted.toLong,
        pbftMessagesInProgressDeleted,
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def getCommits(
      completeBlockMessages: Vector[SignedMessage[PbftNetworkMessage]]
  ): Map[BlockNumber, Vector[SignedMessage[Commit]]] =
    completeBlockMessages
      .collect { case s @ SignedMessage(_: Commit, _) =>
        s.asInstanceOf[SignedMessage[Commit]]
      }
      .groupBy(_.message.blockMetadata.blockNumber)
}

object DbEpochStore {

  private val PrePrepareMessageDiscriminator = 0
  private val PrepareMessageDiscriminator = 1
  private val CommitMessageDiscriminator = 2
  private val ViewChangeDiscriminator = 3
  private val NewViewDiscriminator = 4

  private class InsertBatchAggregatorProcessor(
      exec: (
          Seq[SignedMessage[PbftNetworkMessage]],
          TraceContext,
      ) => FutureUnlessShutdown[Unit],
      override val kind: String,
      override val logger: TracedLogger,
  )(implicit executionContext: ExecutionContext)
      extends BatchAggregator.Processor[SignedMessage[PbftNetworkMessage], Unit] {

    override def executeBatch(
        items: NonEmpty[Seq[Traced[SignedMessage[PbftNetworkMessage]]]]
    )(implicit
        traceContext: TraceContext,
        callerCloseContext: CloseContext,
    ): FutureUnlessShutdown[Iterable[Unit]] =
      exec(items.map(_.value), traceContext)
        .map(_ => Seq.fill(items.size)(()))

    override def prettyItem: Pretty[SignedMessage[PbftNetworkMessage]] = {
      import com.digitalasset.canton.logging.pretty.PrettyUtil.*
      prettyOfClass[SignedMessage[PbftNetworkMessage]](
        param("epoch", _.message.blockMetadata.epochNumber),
        param("block", _.message.blockMetadata.blockNumber),
      )
    }
  }

  private def key[M <: PbftNetworkMessage](
      msg: SignedMessage[M]
  ): (BlockNumber, EpochNumber, BftNodeId, Int) =
    (
      msg.message.blockMetadata.blockNumber,
      msg.message.blockMetadata.epochNumber,
      msg.from,
      getDiscriminator(msg.message),
    )

  private def getDiscriminator[M <: PbftNetworkMessage](message: M): Int =
    message match {
      case _: PrePrepare => PrePrepareMessageDiscriminator
      case _: ConsensusMessage.Prepare => PrepareMessageDiscriminator
      case _: Commit => CommitMessageDiscriminator
      case _: ConsensusMessage.ViewChange => ViewChangeDiscriminator
      case _: ConsensusMessage.NewView => NewViewDiscriminator
    }

}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.db

import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.ConsensusMessage as ProtoConsensusMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.{
  Block,
  Epoch,
  EpochInProgress,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.db.DbEpochStore.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  Genesis,
  OrderedBlocksReader,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftNetworkMessage,
  PbftViewChangeMessage,
  PrePrepare,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterByteString
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import slick.dbio.DBIOAction
import slick.jdbc.{GetResult, PositionedResult, SetParameter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class DbEpochStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends EpochStore[PekkoEnv]
    with OrderedBlocksReader[PekkoEnv]
    with DbStore {

  import storage.api.*

  private val profile = storage.profile

  private implicit val readEpoch: GetResult[EpochInfo] = GetResult { r =>
    EpochInfo(
      EpochNumber(r.nextLong()),
      BlockNumber(r.nextLong()),
      EpochLength(r.nextLong()),
      EffectiveTime(CantonTimestamp.assertFromLong(r.nextLong())),
    )
  }

  private implicit val readPbftMessage: GetResult[PbftNetworkMessage] =
    GetResult(r => parsePbftMessage(r))

  private implicit val tryReadPrePrepareMessage: GetResult[PrePrepare] =
    GetResult(r => tryParsePrePrepareMessage(r))

  private implicit val tryReadPrePrepareMessageAndEpochInfo: GetResult[(PrePrepare, EpochInfo)] =
    GetResult { r =>
      val prePrepare = tryParsePrePrepareMessage(r)
      prePrepare -> readEpoch(r)
    }

  private def tryParsePrePrepareMessage(r: PositionedResult) =
    parsePbftMessage(r) match {
      case prePrepare: PrePrepare => prePrepare
      case m =>
        throw new DbDeserializationException(
          s"Expected to deserialize a pre-prepare message but got: $m"
        )
    }

  private implicit val readCommitMessage: GetResult[Commit] = GetResult { r =>
    parsePbftMessage(r) match {
      case c: Commit => c
      case m =>
        throw new DbDeserializationException(
          s"Expected to deserialize a commit message but got: $m"
        )
    }
  }

  private def createFuture[X](
      actionName: String
  )(future: => Future[X])(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[X] =
    PekkoFutureUnlessShutdown(actionName, storage.performUnlessClosingF(actionName)(future))

  private def parsePbftMessage(r: PositionedResult): PbftNetworkMessage = {
    val messageBytes = r.nextBytes()
    val sequencerIdString = r.nextString()

    val messageOrError = for {
      consensusMessageProto <- Try(ProtoConsensusMessage.parseFrom(messageBytes)).toEither
        .leftMap(x => ProtoDeserializationError.OtherError(x.toString))
      sequencerId <- UniqueIdentifier
        .fromProtoPrimitive(sequencerIdString, "fromSequencerId")
        .map(SequencerId(_))
      consensus <- IssConsensusModule.parseNetworkMessage(
        sequencerId,
        consensusMessageProto,
      )
    } yield consensus

    messageOrError.fold(
      error => throw new DbDeserializationException(s"Could not deserialize pbft message: $error"),
      identity,
    )
  }

  private implicit val setPbftMessagesParameter: SetParameter[PbftNetworkMessage] = { (msg, pp) =>
    pp >> msg.toProto.toByteString
  }

  override def startEpoch(
      epoch: EpochInfo
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(startEpochActionName(epoch)) {
      storage.update_(
        profile match {
          case _: Postgres =>
            sqlu"""insert into ord_epochs(epoch_number, start_block_number, epoch_length, topology_ts, in_progress)
                   values (${epoch.number}, ${epoch.startBlockNumber}, ${epoch.length}, ${epoch.topologySnapshotEffectiveTime.value}, true)
                   on conflict (epoch_number, start_block_number, epoch_length, topology_ts, in_progress) do nothing
                """
          case _: H2 =>
            sqlu"""merge into ord_epochs
                   using dual on (
                     ord_epochs.epoch_number = ${epoch.number}
                     and ord_epochs.start_block_number = ${epoch.startBlockNumber}
                     and ord_epochs.epoch_length = ${epoch.length}
                     and ord_epochs.topology_ts = ${epoch.topologySnapshotEffectiveTime.value}
                     and ord_epochs.in_progress = true
                   )
                   when not matched then
                     insert (epoch_number, start_block_number, epoch_length, topology_ts, in_progress)
                     values (${epoch.number}, ${epoch.startBlockNumber}, ${epoch.length}, ${epoch.topologySnapshotEffectiveTime.value}, true)
                """
          case _ => raiseSupportedDbError
        },
        functionFullName,
      )
    }

  override def completeEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(completeEpochActionName(epochNumber)) {
      storage.update_(
        for {
          // delete all in-progress messages after an epoch ends and before we start adding new messages in the new epoch
          _ <- sqlu"delete from ord_pbft_messages_in_progress"
          _ <- sqlu"""update ord_epochs set in_progress = false
                      where epoch_number = $epochNumber
                      """
        } yield (),
        functionFullName,
      )
    }

  private def raiseSupportedDbError =
    sys.error("only Postgres and H2 are supported")

  override def latestEpoch(includeInProgress: Boolean)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Epoch] = createFuture(latestCompletedEpochActionName) {
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
            sql"""select message, from_sequencer_id
                  from ord_pbft_messages_completed pbft_message
                  where pbft_message.block_number = ${epoch.lastBlockNumber} and pbft_message.discriminator = $CommitMessageDiscriminator
                  order by pbft_message.from_sequencer_id
               """.as[Commit]
        } yield Epoch(epoch, lastBlockCommitMessages),
        functionFullName,
      )
  }

  override def addPrePrepare(prePrepare: ConsensusMessage.PrePrepare)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = createFuture(addPrePrepareActionName(prePrepare)) {
    storage.update_(
      DBIOAction.sequence(insertInProgressPbftMessages(Seq(prePrepare))),
      functionFullName,
    )
  }

  override def addPrepares(
      prepares: Seq[ConsensusMessage.Prepare]
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(addPreparesActionName) {
      storage.update_(
        DBIOAction.sequence(insertInProgressPbftMessages(prepares)).transactionally,
        functionFullName,
      )
    }

  override def addViewChangeMessage[M <: PbftViewChangeMessage](viewChangeMessage: M)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] =
    createFuture(addViewChangeMessageActionName(viewChangeMessage)) {
      storage.update_(
        DBIOAction.sequence(insertInProgressPbftMessages(Seq(viewChangeMessage))).transactionally,
        functionFullName,
      )
    }

  override def addOrderedBlock(
      prePrepare: PrePrepare,
      commitMessages: Seq[Commit],
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val epochNumber = prePrepare.blockMetadata.epochNumber
    val blockNumber = prePrepare.blockMetadata.blockNumber
    createFuture(addOrderedBlockActionName(epochNumber, blockNumber)) {
      storage.update_(
        DBIOAction
          .sequence(
            cleanUpInProgressMessages(blockNumber) +: (insertFinalPbftMessages(
              commitMessages
            ) ++ insertFinalPbftMessages(Seq(prePrepare)))
          )
          .transactionally,
        functionFullName,
      )
    }
  }

  private def cleanUpInProgressMessages(blockNumber: Long) =
    // delete pre-prepares and prepares, but not view change messages
    sqlu"delete from ord_pbft_messages_in_progress where block_number=$blockNumber and discriminator <= $PrepareMessageDiscriminator"

  private def insertInProgressPbftMessages[M <: PbftNetworkMessage](
      messages: Seq[M]
  ): Seq[DbAction.WriteOnly[Int]] =
    messages.headOption.fold(Seq.empty[DbAction.WriteOnly[Int]]) { head =>
      val discriminator = getDiscriminator(head)
      messages.map { msg =>
        val sequencerId = msg.from.uid.toProtoPrimitive
        val blockNumber = msg.blockMetadata.blockNumber
        val epochNumber = msg.blockMetadata.epochNumber
        val viewNumber = msg.viewNumber

        profile match {
          case _: Postgres =>
            sqlu"""insert into ord_pbft_messages_in_progress(block_number, epoch_number, view_number, message, discriminator, from_sequencer_id)
                   values ($blockNumber, $epochNumber, $viewNumber, $msg, $discriminator, $sequencerId)
                   on conflict (block_number, view_number, discriminator, from_sequencer_id) do nothing
                """
          case _: H2 =>
            sqlu"""merge into ord_pbft_messages_in_progress
                   using dual on (ord_pbft_messages_in_progress.block_number = $blockNumber
                     and ord_pbft_messages_in_progress.epoch_number = $epochNumber
                     and ord_pbft_messages_in_progress.view_number = $viewNumber
                     and ord_pbft_messages_in_progress.discriminator = $discriminator
                     and ord_pbft_messages_in_progress.from_sequencer_id = $sequencerId
                   )
                   when not matched then
                     insert (block_number, epoch_number, view_number, message, discriminator, from_sequencer_id)
                     values ($blockNumber, $epochNumber, $viewNumber, $msg, $discriminator, $sequencerId)
                """
          case _ => raiseSupportedDbError
        }
      }
    }

  private def insertFinalPbftMessages[M <: PbftNetworkMessage](
      messages: Seq[M]
  ): Seq[DbAction.WriteOnly[Int]] =
    messages.headOption.fold(Seq.empty[DbAction.WriteOnly[Int]]) { head =>
      val discriminator = getDiscriminator(head)
      messages.map { msg =>
        val sequencerId = msg.from.uid.toProtoPrimitive
        val blockNumber = msg.blockMetadata.blockNumber
        val epochNumber = msg.blockMetadata.epochNumber

        profile match {
          case _: Postgres =>
            sqlu"""insert into ord_pbft_messages_completed(block_number, epoch_number, message, discriminator, from_sequencer_id)
                   values ($blockNumber, $epochNumber, $msg, $discriminator, $sequencerId)
                   on conflict (block_number, epoch_number, discriminator, from_sequencer_id) do nothing
                """
          case _: H2 =>
            sqlu"""merge into ord_pbft_messages_completed
                   using dual on (ord_pbft_messages_completed.block_number = $blockNumber
                     and ord_pbft_messages_completed.epoch_number = $epochNumber
                     and ord_pbft_messages_completed.discriminator = $discriminator
                     and ord_pbft_messages_completed.from_sequencer_id = $sequencerId
                   )
                   when not matched then
                     insert (block_number, epoch_number, message, discriminator, from_sequencer_id)
                     values ($blockNumber, $epochNumber, $msg, $discriminator, $sequencerId)
                """
          case _ => raiseSupportedDbError
        }
      }
    }

  override def loadEpochProgress(activeEpochInfo: EpochInfo)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[EpochInProgress] =
    createFuture(loadEpochProgressActionName(activeEpochInfo)) {
      storage
        .query(
          for {
            completeBlockMessages <-
              sql"""select message, from_sequencer_id
                    from ord_pbft_messages_completed
                    where epoch_number = ${activeEpochInfo.number}
                    order by from_sequencer_id, block_number
                 """.as[PbftNetworkMessage](readPbftMessage)
            pbftMessagesForIncompleteBlocks <-
              sql"""select message, from_sequencer_id
                    from ord_pbft_messages_in_progress
                    where epoch_number = ${activeEpochInfo.number}
                    order by from_sequencer_id, block_number, discriminator, view_number
                 """
                // had to set the GetResult explicitly because for some reason it was picking the one for commit messages
                .as[PbftNetworkMessage](readPbftMessage)
          } yield {
            val commits = completeBlockMessages
              .collect { case commit: Commit =>
                commit
              }
              .groupBy(_.blockMetadata.blockNumber)
            val sortedBlocks = completeBlockMessages
              .collect { case pp: PrePrepare =>
                val blockNumber = pp.blockMetadata.blockNumber
                Block(
                  activeEpochInfo.number,
                  blockNumber,
                  CommitCertificate(pp, commits.getOrElse(blockNumber, Seq.empty)),
                )
              }
              .sortBy(_.blockNumber)
            EpochInProgress(sortedBlocks, pbftMessagesForIncompleteBlocks)
          },
          functionFullName,
        )
    }

  override def loadPrePreparesForCompleteBlocks(
      startEpochNumberInclusive: EpochNumber,
      endEpochNumberInclusive: EpochNumber,
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Seq[PrePrepare]] =
    createFuture(loadPrePreparesActionName(startEpochNumberInclusive, endEpochNumberInclusive)) {
      storage
        .query(
          sql"""select completed_message.message, completed_message.from_sequencer_id
                from ord_pbft_messages_completed completed_message
                where completed_message.discriminator = $PrePrepareMessageDiscriminator
                  and completed_message.epoch_number >= $startEpochNumberInclusive
                  and completed_message.epoch_number <= $endEpochNumberInclusive
                order by completed_message.block_number
             """.as[PrePrepare](tryReadPrePrepareMessage),
          functionFullName,
        )
    }

  override def loadOrderedBlocks(
      initialBlockNumber: BlockNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Seq[OrderedBlockForOutput]] =
    createFuture(loadOrderedBlocksActionName(initialBlockNumber)) {
      storage
        .query(
          sql"""select
                  completed_message.message, completed_message.from_sequencer_id,
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
              prePrepare.from,
              epochInfo.lastBlockNumber == prePrepare.blockMetadata.blockNumber,
            )
          }
        }
    }
}

object DbEpochStore {
  private val PrePrepareMessageDiscriminator = 0
  private val PrepareMessageDiscriminator = 1
  private val CommitMessageDiscriminator = 2
  private val ViewChangeDiscriminator = 3
  private val NewViewDiscriminator = 4

  private def getDiscriminator[M <: PbftNetworkMessage](message: M): Int =
    message match {
      case _: PrePrepare => PrePrepareMessageDiscriminator
      case _: ConsensusMessage.Prepare => PrepareMessageDiscriminator
      case _: Commit => CommitMessageDiscriminator
      // TODO(#16820): Finalize DB persistence logic for these view change message types
      case _: ConsensusMessage.ViewChange => ViewChangeDiscriminator
      case _: ConsensusMessage.NewView => NewViewDiscriminator
    }
}

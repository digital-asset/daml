// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.PbftMessageValidator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

import IssConsensusModuleMetrics.emitNonCompliance
import PbftBlockState.{
  PbftBlockStateAction,
  ProcessResult,
  SendPbftMessage,
  SignPbftMessage,
  StorePrePrepare,
  StorePrepares,
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class PbftBlockState(
    membership: Membership,
    clock: Clock,
    messageValidator: PbftMessageValidator,
    leader: SequencerId,
    epoch: EpochNumber,
    view: ViewNumber,
    firstInSegment: Boolean,
    abort: String => Nothing,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit mc: MetricsContext)
    extends NamedLogging {

  // Convenience val for various checks
  private val isLeaderOfThisView: Boolean = membership.myId == leader
  private val isInitialView: Boolean = view == ViewNumber.First

  // In-memory storage for block's PBFT votes in this view
  private var prePrepare: Option[SignedMessage[PrePrepare]] = None
  private val prepareMap = mutable.HashMap[SequencerId, SignedMessage[Prepare]]()
  private val commitMap = mutable.HashMap[SequencerId, SignedMessage[Commit]]()

  // TRUE when PrePrepare is stored
  private var prePrepareStored: Boolean = false
  // TRUE when Quorum of Prepares is stored
  private var preparesStored: Boolean = false

  private def pbftAction(condition: TraceContext => Boolean)(
      action: (Hash, SignedMessage[PrePrepare]) => TraceContext => Seq[ProcessResult]
  ): PbftBlockStateAction[Unit] =
    new PbftBlockStateAction(
      traceContext => Option.when(condition(traceContext))(()),
      { case (_, ppHash, pp) =>
        action(ppHash, pp)
      },
    )

  private def pbftActionOpt[ConditionResult](condition: TraceContext => Option[ConditionResult])(
      action: (
          ConditionResult,
          Hash,
          SignedMessage[PrePrepare],
      ) => TraceContext => Seq[ProcessResult]
  ): PbftBlockStateAction[ConditionResult] =
    new PbftBlockStateAction(condition, action)

  private val prePrepareAction =
    pbftAction(_ => isInitialView && isLeaderOfThisView && prePrepare.isDefined) { case (_, pp) =>
      _ =>
        Seq(
          SendPbftMessage(
            pp,
            // on views other than the original one, the pre-prepares are stored in new-view messages
            store = Some(StorePrePrepare(pp)),
          )
        )
    }

  private val createPrepareAction =
    pbftAction(_ => prePrepare.isDefined) { case (hash, pp) =>
      _ =>
        if (!prepareMap.contains(membership.myId)) {
          val prepare =
            Prepare.create(pp.message.blockMetadata, view, hash, clock.now, membership.myId)
          Seq(SignPbftMessage(prepare))
        } else {
          // We already have a Prepare (potentially from rehydration) so we don't need to generate a new one
          Seq.empty
        }
    }

  private val sendPrepareAction =
    pbftActionOpt[SignedMessage[Prepare]] { _ =>
      for {
        _ <- prePrepare
        prepare <- prepareMap.get(membership.myId)
      } yield prepare
    } { case (prepare, _, pp) =>
      _ =>
        Seq(
          SendPbftMessage(
            prepare,
            store = Option.when(!isLeaderOfThisView && isInitialView)(StorePrePrepare(pp)),
          )
        )
    }

  private val createCommitAction = pbftAction(implicit traceContext =>
    prePrepare.fold(false) { pp =>
      val hasReachedQuorumOfPrepares = {
        val hash = pp.message.hash
        val (matchingHash, nonMatchingHash) = prepareMap.values.partition(_.message.hash == hash)
        if (nonMatchingHash.nonEmpty)
          logger.warn(
            s"Found non-matching hashes for prepare messages from peers (${nonMatchingHash.map(_.from)})"
          )
        matchingHash.sizeIs >= membership.orderingTopology.strongQuorum
      }
      // We send our local commit when all the following conditions are true:
      //   1. the complete action has NOT yet fired
      //       if the complete action has fired, the block is complete, we have a commit certificate,
      //       and the segment may already be cleaned up, so we want to avoid unnecessary async events
      //   2. we reached a quorum of (valid, matching) prepares for the first time
      //   3. we confirmed storage of the corresponding PrePrepare, which needs to finish before
      //      storing the quorum of prepares and broadcasting our local commit
      !completeAction.isFired && hasReachedQuorumOfPrepares && prePrepareStored
    }
  ) { case (hash, pp) =>
    _ =>
      val commit = Commit.create(pp.message.blockMetadata, view, hash, clock.now, membership.myId)
      Seq(SignPbftMessage(commit))
  }

  private val sendCommitAction = pbftActionOpt(_ => commitMap.get(membership.myId)) {
    (commit, _, _) => _ =>
      Seq(
        SendPbftMessage(
          commit,
          store = Some(StorePrepares(prepareMap.values.toSeq.sortBy(_.from))),
        )
      )
  }

  private val completeAction = pbftAction(implicit traceContext =>
    prePrepare.fold(false) { pp =>
      val hasReachedQuorumOfCommits = {
        val hash = pp.message.hash
        val (matchingHash, nonMatchingHash) = commitMap.values.partition(_.message.hash == hash)
        if (nonMatchingHash.nonEmpty)
          logger.warn(
            s"Found non-matching hashes for commit messages from peers (${nonMatchingHash.map(_.from)})"
          )
        matchingHash.sizeIs >= membership.orderingTopology.strongQuorum
      }
      val allNecessaryVotesStored =
        prePrepareStored && (preparesStored || !commitMap.contains(membership.myId))
      // We complete ordering for this block when all the following conditions are true:
      //   1. we reached a quorum of (valid, matching) commits for the first time
      //   2. all necessary Pbft votes have been stored. This is always the PrePrepare and,
      //      if we sent a local commit earlier, also the matching set of Prepares
      hasReachedQuorumOfCommits && allNecessaryVotesStored
    }
  ) { case (_, _) => _ => Seq.empty }

  /** Processes a normal case (i.e., not a view change) PBFT message for the block in progress.
    * @return `true` if the state has been updated; it's the only case when `advance` should be called,
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def processMessage(
      msg: SignedMessage[PbftNormalCaseMessage]
  )(implicit traceContext: TraceContext): Boolean =
    msg.message match {
      case _: PrePrepare =>
        setPrePrepare(msg.asInstanceOf[SignedMessage[PrePrepare]])

      case _: Prepare =>
        addPrepare(msg.asInstanceOf[SignedMessage[Prepare]])

      case _: Commit =>
        addCommit(msg.asInstanceOf[SignedMessage[Commit]])
    }

  /** Advances the block processing state.
    * It should be called only after the state has been updated by `processMessage`.
    * @return a sequence of actions to be taken as a result of the state change; the order of such actions
    *         only depends on the state.
    */
  def advance()(implicit traceContext: TraceContext): Seq[ProcessResult] =
    prePrepare
      .map { pp =>
        val hash = pp.message.hash

        Seq(
          prePrepareAction,
          createPrepareAction,
          sendPrepareAction,
          createCommitAction,
          sendCommitAction,
          completeAction,
        )
          .flatMap { action =>
            action.run(hash, pp)
          }
      }
      .getOrElse(Seq.empty)

  def prepareVoters: Iterable[SequencerId] = prepareMap.keys

  def commitVoters: Iterable[SequencerId] = commitMap.keys

  private def setPrePrepare(pp: SignedMessage[PrePrepare])(implicit
      traceContext: TraceContext
  ): Boolean =
    // Only PrePrepares contained in a NewView message (during a view change) will result in pp.view != view
    // In this case, we want to store "old" PrePrepares (from previous views and previous leaders) in this block
    // We rely on validation of the NewView message to safely bootstrap this block with a valid PrePrepare

    if (pp.message.viewNumber == view && pp.from != leader) {
      emitNonCompliance(metrics)(
        pp.from,
        epoch,
        view,
        pp.message.blockMetadata.blockNumber,
        metrics.security.noncompliant.labels.violationType.values.ConsensusRoleEquivocation,
      )
      logger.warn(
        s"PrePrepare for block ${pp.message.blockMetadata.blockNumber} from wrong peer (${pp.from}), " +
          s"should be from $leader"
      )
      false
    } else if (prePrepare.isDefined) {
      logger.info(
        s"PrePrepare for block ${pp.message.blockMetadata.blockNumber} already exists; ignoring new one"
      )
      false
    } else {
      val shouldAdvance = messageValidator
        .validatePrePrepare(pp.message, firstInSegment)
        .fold(
          { error =>
            logger.warn(s"PrePrepare validation failed with: '$error', dropping...")
            false
          },
          { _ =>
            prePrepare = Some(pp)
            true
          },
        )
      shouldAdvance
    }

  private def addPrepare(
      p: SignedMessage[Prepare]
  )(implicit traceContext: TraceContext): Boolean =
    prepareMap.get(p.from) match {
      case Some(prepare) =>
        val baseLogMsg =
          s"Prepare for block ${p.message.blockMetadata.blockNumber} already exists from peer ${p.from};"
        if (prepare.message.hash != p.message.hash) {
          emitNonCompliance(metrics)(
            p.from,
            epoch,
            view,
            p.message.blockMetadata.blockNumber,
            metrics.security.noncompliant.labels.violationType.values.ConsensusDataEquivocation,
          )
          logger.warn(
            s"$baseLogMsg stored Prepare has hash ${prepare.message.hash}, found different hash ${p.message.hash}"
          )
        } else {
          logger.info(s"$baseLogMsg new Prepare has matching hash (${prepare.message.hash})")
        }
        false
      case None =>
        prepareMap.put(p.from, p).discard
        true
    }

  private def addCommit(c: SignedMessage[Commit])(implicit traceContext: TraceContext): Boolean =
    commitMap.get(c.from) match {
      case Some(commit) =>
        val baseLogMsg =
          s"Commit for block ${c.message.blockMetadata.blockNumber} already exists from peer ${c.from}; "
        if (commit.message.hash != c.message.hash) {
          emitNonCompliance(metrics)(
            c.from,
            epoch,
            view,
            c.message.blockMetadata.blockNumber,
            metrics.security.noncompliant.labels.violationType.values.ConsensusDataEquivocation,
          )
          logger.warn(
            s"$baseLogMsg stored Commit has hash ${commit.message.hash}, found different hash ${c.message.hash}"
          )
        } else {
          logger.info(s"$baseLogMsg new Commit has matching hash (${commit.message.hash})")
        }
        false
      case None =>
        commitMap.put(c.from, c).discard
        true
    }

  /** @return Boolean signaling whether that block has completed consensus, not including storage of messages
    */
  def isBlockComplete: Boolean = completeAction.isFired

  /** Confirm that the pre-prepare for this block has been stored
    */
  def confirmPrePrepareStored(): Unit =
    if (createPrepareAction.isFired)
      prePrepareStored = true
    else
      abort("UnPrePrepared block should not have had pre-prepare stored")

  /** Confirm that the quorum of prepares for this block has been stored
    */
  def confirmPreparesStored(): Unit =
    if (createCommitAction.isFired)
      preparesStored = true
    else abort("UnPrepared block should not have had prepares stored")

  private def commitMessageQuorum: Seq[SignedMessage[Commit]] = {
    val hash =
      prePrepare
        .getOrElse(abort("The block is not complete (there is no PrePrepare)"))
        .message
        .hash
    val commitsMatchingHash = commitMap.view.values.filter(_.message.hash == hash)
    // Assumes that commits are already validated.
    // Sorting here has is not strictly needed for correctness, but it improves order stability in tests.
    commitsMatchingHash.toSeq.sorted.take(membership.orderingTopology.strongQuorum)
  }

  /** @return Commit certificate is defined if the block has completed consensus
    */
  def commitCertificate: Option[CommitCertificate] =
    if (completeAction.isFired) {
      prePrepare.map(pp =>
        CommitCertificate(
          pp,
          commitMessageQuorum,
        )
      )
    } else
      None

  /** @return Prepare certificate is defined if the block has reached the prepared state and the prepares have been stored
    */
  def prepareCertificate: Option[ConsensusCertificate] =
    if (preparesStored)
      prePrepare.map(pp =>
        PrepareCertificate(
          pp,
          prepareMap.values
            .filter(_.message.hash == pp.message.hash)
            .toSeq
            .sortBy(_.message.from)
            .take(membership.orderingTopology.strongQuorum),
        )
      )
    else
      None

  def status: ConsensusStatus.BlockStatus =
    // for the purposes of retransmission status we are complete once we are commited,
    // even if we haven't stored the commits yet, because we don't need to request for any more messages
    if (completeAction.isFired) ConsensusStatus.BlockStatus.Complete
    else
      ConsensusStatus.BlockStatus.InProgress(
        prePrepare.isDefined,
        membership.sortedPeers.map(prepareMap.contains),
        membership.sortedPeers.map(commitMap.contains),
      )

  def messagesToRetransmit(
      fromStatus: ConsensusStatus.BlockStatus.InProgress
  ): Seq[SignedMessage[PbftNetworkMessage]] = {
    val ConsensusStatus.BlockStatus.InProgress(
      fromPrePrepared,
      remotePreparesPresent,
      remoteCommitsPresent,
    ) = fromStatus
    val missingPrePrepare =
      if (
        !fromPrePrepared
        && isInitialView // for views later than the first, the pre-prepare will be included in the new-view message
        && (prePrepareStored || !isLeaderOfThisView) // if we're the leader, we only retransmit if we've stored the pre-prepare (the initial send also follows this rule)
      )
        prePrepare.toList
      else Seq.empty[SignedMessage[PbftNetworkMessage]]

    def missingMessages[M <: PbftNetworkMessage](
        remoteHasIt: Seq[Boolean],
        myMessages: collection.Map[SequencerId, SignedMessage[M]],
        canIncludeLocalMessage: Boolean,
    ): Seq[SignedMessage[PbftNetworkMessage]] = membership.sortedPeers.view
      .zip(remoteHasIt)
      .flatMap { case (node, hasMsg) =>
        if (hasMsg) None
        else if (membership.myId == node) {
          if (canIncludeLocalMessage) myMessages.get(node) else None
        } else myMessages.get(node)
      }
      // take only enough to complete strong quorum
      .take(membership.orderingTopology.strongQuorum - remoteHasIt.count(identity))
      .toSeq

    missingPrePrepare ++
      // we only send local prepare after storing the pre-prepare, so we also should only allow retransmission under the same condition
      missingMessages(remotePreparesPresent, prepareMap, prePrepareStored) ++
      // we only send local commit after storing the prepare quorum, so we also should only allow retransmission under the same condition
      missingMessages(remoteCommitsPresent, commitMap, preparesStored)
  }
}

object PbftBlockState {

  sealed trait ProcessResult extends Product with Serializable
  final case class SignPbftMessage[MessageT <: PbftNetworkMessage](
      pbftMessage: MessageT
  ) extends ProcessResult
  final case class SignPrePreparesForNewView(
      blockMetadata: BlockMetadata,
      viewNumber: ViewNumber,
      prePrepares: Seq[Either[PrePrepare, SignedMessage[PrePrepare]]],
  ) extends ProcessResult

  final case class SendPbftMessage[MessageT <: PbftNetworkMessage](
      pbftMessage: SignedMessage[MessageT],
      store: Option[StoreResult],
  ) extends ProcessResult
  final case class CompletedBlock(
      commitCertificate: CommitCertificate,
      viewNumber: ViewNumber,
  ) extends ProcessResult
  final case class ViewChangeStartNestedTimer(blockMetadata: BlockMetadata, viewNumber: ViewNumber)
      extends ProcessResult

  /** Indicates that the view change was successfully completed.
    * If this node is the leader of this view, it will have sent the new view message and stored it as part of that.
    * If this node is not the leader, the [[store]] will be populated with the new view message that was already validate and should be
    * then stored for crash recovery purposes.
    */
  final case class ViewChangeCompleted(
      blockMetadata: BlockMetadata,
      viewNumber: ViewNumber,
      store: Option[StoreViewChangeMessage[NewView]],
  ) extends ProcessResult

  sealed trait StoreResult extends Product with Serializable
  final case class StorePrePrepare(prePrepare: SignedMessage[PrePrepare]) extends StoreResult
  final case class StorePrepares(prepares: Seq[SignedMessage[Prepare]]) extends StoreResult
  final case class StoreViewChangeMessage[M <: PbftViewChangeMessage](
      viewChangeMessage: SignedMessage[M]
  ) extends StoreResult

  private final class PbftBlockStateAction[ConditionResult](
      preCondition: TraceContext => Option[ConditionResult],
      action: (
          ConditionResult,
          Hash,
          SignedMessage[PrePrepare],
      ) => TraceContext => Seq[ProcessResult],
  ) {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var fired: Boolean = false

    def isFired: Boolean = fired

    def run(ppHash: Hash, pp: SignedMessage[PrePrepare])(implicit
        traceContext: TraceContext
    ): Seq[ProcessResult] =
      if (!fired) {
        preCondition(traceContext) match {
          case Some(value) =>
            fired = true
            action(value, ppHash, pp)(traceContext)
          case None =>
            Seq.empty
        }
      } else {
        Seq.empty
      }
  }
}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Hash, Signature}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.PbftBlockState.ProcessResult
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

sealed trait PbftBlockState extends NamedLogging {
  val leader: SequencerId

  /** @return Boolean signaling whether any state has changed as a result of processing this message,
    *         in which case, advance is expected to be called next.
    */
  def processMessage(msg: SignedMessage[PbftNormalCaseMessage])(implicit
      traceContext: TraceContext
  ): Boolean

  /** @return A sequence of results based on the current state of the PBFT process describing
    *         what should be done next in order to advance it towards completing this block.
    */
  def advance()(implicit traceContext: TraceContext): Seq[ProcessResult]

  /** Confirm that the pre-prepare for this block has been stored
    */
  def confirmPrePrepareStored(): Unit

  /** Confirm that the quorum of prepares for this block has been stored
    */
  def confirmPreparesStored(): Unit

  /** Confirm that the commit quorum and pre-prepare for this block have been stored and it is thus completed
    */
  def confirmCompleteBlockStored(): Unit

  /** @return Boolean signaling whether that block has completed consensus, including storage of commit messages and pre-prepare
    */
  def isBlockComplete: Boolean

  /** @return Quorum of commit messages if the block has reached a quorum.
    *         If called before that, this will abort with an error
    */
  def commitMessageQuorum: Seq[SignedMessage[Commit]]

  /** @return A consensus certificate which will be a commit certificate if the block is complete,
    *         otherwise a prepare certificate if the block has at least stored the pre-prepare and prepare quorum,
    *         and none otherwise
    */
  def consensusCertificate: Option[ConsensusCertificate]

  def status: ConsensusStatus.BlockStatus

  def messagesToRetransmit(
      fromStatus: ConsensusStatus.BlockStatus.InProgress
  ): Seq[SignedMessage[PbftNetworkMessage]]

}

object PbftBlockState {

  private final class PbftBlockStateAction(
      preCondition: TraceContext => Boolean,
      action: (Hash, SignedMessage[PrePrepare]) => TraceContext => Seq[ProcessResult],
  ) {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var fired: Boolean = false

    def isFired: Boolean = fired

    def run(ppHash: Hash, pp: SignedMessage[PrePrepare])(implicit
        traceContext: TraceContext
    ): Seq[ProcessResult] =
      if (!fired && preCondition(traceContext)) {
        fired = true
        action(ppHash, pp)(traceContext)
      } else {
        Seq.empty
      }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  final class InProgress(
      membership: Membership,
      clock: Clock,
      override val leader: SequencerId,
      epoch: EpochNumber,
      view: ViewNumber,
      abort: String => Nothing,
      metrics: BftOrderingMetrics,
      override val loggerFactory: NamedLoggerFactory,
  )(implicit mc: MetricsContext)
      extends PbftBlockState {

    // Convenience val for various checks
    private val isLeaderOfThisView: Boolean = membership.myId == leader
    private val isInitialView: Boolean = view == ViewNumber.First

    // In-memory storage for block's PBFT votes in this view
    private var prePrepare: Option[SignedMessage[PrePrepare]] = None
    private val prepareMap = mutable.HashMap[SequencerId, SignedMessage[Prepare]]()
    private var myCommit: Option[Commit] = None
    private val commitMap = mutable.HashMap[SequencerId, SignedMessage[Commit]]()

    // TRUE when PrePrepare is stored
    private var prePrepareStored: Boolean = false
    // TRUE when Quorum of Prepares is stored
    private var preparesStored: Boolean = false
    // TRUE when storage solution acknowledges CompletedBlock was persisted
    private var blockComplete: Boolean = false

    private def pbftAction(condition: TraceContext => Boolean)(
        action: (Hash, SignedMessage[PrePrepare]) => TraceContext => Seq[ProcessResult]
    ): PbftBlockStateAction = new PbftBlockStateAction(condition, action)

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

    private val prepareAction = pbftAction(_ => prePrepare.isDefined) { case (hash, pp) =>
      implicit traceContext =>
        val prepare = prepareMap
          .getOrElse(
            // if our prepare has been restored after a crash,
            // we don't create a different one but rather take existing one
            membership.myId, {
              val p = SignedMessage(
                Prepare
                  .create(pp.message.blockMetadata, view, hash, clock.now, membership.myId),
                Signature.noSignature, // TODO(#20458) actually sign the message
              )
              addPrepare(p).discard
              p
            },
          )
        Seq(
          SendPbftMessage(
            prepare,
            store = Option.when(!isLeaderOfThisView && isInitialView)(StorePrePrepare(pp)),
          )
        )
    }

    private val commitAction = pbftAction(implicit traceContext =>
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
        // we send our local commit if we have reached a quorum of prepares for the first time.
        // Also, because we store the quorum of prepares together with sending this commit,
        // we only want to do it after we've stored the pre-prepare
        hasReachedQuorumOfPrepares && prePrepareStored
      }
    ) { case (hash, pp) =>
      implicit traceContext =>
        val commit =
          SignedMessage(
            Commit.create(pp.message.blockMetadata, view, hash, clock.now, membership.myId),
            Signature.noSignature, // TODO(#20458)
          )
        myCommit = Some(commit.message)
        addCommit(commit).discard
        Seq(
          SendPbftMessage(
            commit,
            store = Some(StorePrepares(prepareMap.values.toSeq.sortBy(_.from))),
          )
        )
    }

    private val completeAction = pbftAction(implicit traceContext =>
      prePrepare.fold(false) { pp =>
        val hash = pp.message.hash
        val (matchingHash, nonMatchingHash) = commitMap.values.partition(_.message.hash == hash)
        val result =
          matchingHash.sizeIs >= membership.orderingTopology.strongQuorum && myCommit.isDefined && preparesStored
        if (nonMatchingHash.nonEmpty)
          logger.warn(
            s"Found non-matching hashes for commit messages from peers (${nonMatchingHash.map(_.from)})"
          )
        result
      }
    ) { case (_, pp) => _ => Seq(CompletedBlock(pp, commitMessageQuorum, view)) }

    /** Processes a normal case (i.e., not a view change) PBFT message for the block in progress.
      * @return `true` if the state has been updated; it's the only case when `advance` should be called,
      */
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    override def processMessage(
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
    override def advance()(implicit traceContext: TraceContext): Seq[ProcessResult] =
      prePrepare
        .map { pp =>
          val hash = pp.message.hash

          Seq(prePrepareAction, prepareAction, commitAction, completeAction).flatMap { action =>
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
        // TODO(#17108): verify PrePrepare is sound in terms of ProofsOfAvailability
        // TODO(i18194) check signatures of ProofsOfAvailability
        prePrepare = Some(pp)
        true
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

    override def isBlockComplete: Boolean = blockComplete

    override def confirmPrePrepareStored(): Unit =
      if (prepareAction.isFired)
        prePrepareStored = true
      else
        abort("UnPrePrepared block should not have had pre-prepare stored")

    override def confirmPreparesStored(): Unit =
      if (commitAction.isFired)
        preparesStored = true
      else abort("UnPrepared block should not have had prepares stored")

    override def confirmCompleteBlockStored(): Unit =
      if (completeAction.isFired)
        blockComplete = true
      else
        abort("Uncommitted block shouldn't be stored")

    override def commitMessageQuorum: Seq[SignedMessage[Commit]] = {
      val hash =
        prePrepare
          .getOrElse(abort("The block is not complete (there is no PrePrepare)"))
          .message
          .hash
      if (completeAction.isFired) {
        val commitsMatchingHash = commitMap.view.values.filter(_.message.hash == hash)
        // Assumes that commits are already validated.
        // Sorting here has is not strictly needed for correctness, but it improves order stability in tests.
        commitsMatchingHash.toSeq.sorted.take(membership.orderingTopology.strongQuorum)
      } else abort("The block is not complete (there are not 2f+1 commit messages yet)")
    }

    override def consensusCertificate: Option[ConsensusCertificate] =
      if (blockComplete) {
        prePrepare.map(pp =>
          CommitCertificate(
            pp,
            commitMap.values
              .filter(_.message.hash == pp.message.hash)
              .toSeq
              .sortBy(_.from)
              .take(membership.orderingTopology.strongQuorum),
          )
        )
      } else if (preparesStored)
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

    override def status: ConsensusStatus.BlockStatus =
      // for the purposes of retransmission status we are complete once we are commited,
      // even if we haven't stored the commits yet, because we don't need to request for any more messages
      if (completeAction.isFired) ConsensusStatus.BlockStatus.Complete
      else
        ConsensusStatus.BlockStatus.InProgress(
          prePrepare.isDefined,
          membership.sortedPeers.map(prepareMap.contains),
          membership.sortedPeers.map(commitMap.contains),
        )

    override def messagesToRetransmit(
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
          shouldIncludeLocalMessage: Boolean,
      ): Seq[SignedMessage[PbftNetworkMessage]] = membership.sortedPeers.view
        .zip(remoteHasIt)
        .flatMap { case (node, hasMsg) =>
          if (hasMsg) None
          else if (membership.myId == node) {
            if (shouldIncludeLocalMessage) myMessages.get(node) else None
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

  final class AlreadyOrdered(
      override val leader: SequencerId,
      commitCertificate: CommitCertificate,
      override val loggerFactory: NamedLoggerFactory,
  ) extends PbftBlockState {
    override def processMessage(
        msg: SignedMessage[PbftNormalCaseMessage]
    )(implicit traceContext: TraceContext): Boolean = {
      val messageType = shortType(msg)
      logger.info(
        s"Block ${msg.message.blockMetadata.blockNumber} is complete, " +
          s"so ignoring message $messageType from peer ${msg.from}"
      )
      // no state change can occur for a block that has already been ordered
      false
    }

    override def advance()(implicit traceContext: TraceContext): Seq[ProcessResult] = Seq.empty
    override def isBlockComplete: Boolean = true
    override def confirmPrePrepareStored(): Unit = ()
    override def confirmPreparesStored(): Unit = ()
    override def confirmCompleteBlockStored(): Unit = ()
    override def commitMessageQuorum: Seq[SignedMessage[Commit]] = commitCertificate.commits
    override def consensusCertificate: Option[ConsensusCertificate] = Some(commitCertificate)
    override def status: ConsensusStatus.BlockStatus =
      ConsensusStatus.BlockStatus.Complete

    override def messagesToRetransmit(
        fromStatus: ConsensusStatus.BlockStatus.InProgress
    ): Seq[SignedMessage[PbftNetworkMessage]] = Seq.empty

  }

  sealed trait ProcessResult extends Product with Serializable
  final case class SendPbftMessage[MessageT <: PbftNetworkMessage](
      pbftMessage: SignedMessage[MessageT],
      store: Option[StoreResult],
  ) extends ProcessResult
  final case class CompletedBlock(
      prePrepare: SignedMessage[PrePrepare],
      commitMessageQuorum: Seq[SignedMessage[Commit]],
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
}

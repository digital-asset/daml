// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.Signature
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
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

sealed trait PbftBlockState extends NamedLogging {
  val leader: SequencerId
  def processMessage(msg: SignedMessage[PbftNormalCaseMessage])(implicit
      traceContext: TraceContext
  ): Boolean
  def advance()(implicit traceContext: TraceContext): Seq[ProcessResult]
  def isBlockComplete: Boolean
  def confirmCompleteBlockStored(): Unit
  def commitMessageQuorum: Seq[SignedMessage[Commit]]
  def consensusCertificate: Option[ConsensusCertificate]
}

object PbftBlockState {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class InProgress(
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

    // In-memory storage for block's PBFT votes in this view
    private var prePrepare: Option[SignedMessage[PrePrepare]] = None
    private var sentPrePrepare: Boolean = false
    private var myPrepare: Option[SignedMessage[Prepare]] = None
    private val prepareMap = mutable.HashMap[SequencerId, SignedMessage[Prepare]]()
    private var myCommit: Option[Commit] = None
    private val commitMap = mutable.HashMap[SequencerId, SignedMessage[Commit]]()

    // TRUE when PrePrepare is sent (leader-only) AND Prepare is sent (all peers)
    private var prePrepared: Boolean = false
    // TRUE when Quorum of Prepares received AND PrePrepare.defined
    private var prepared: Boolean = false
    // TRUE when Quorum of Commits received AND MyCommit.Defined AND PrePrepare.defined
    private var committed: Boolean = false
    // TRUE when storage solution acknowledges CompletedBlock was persisted
    private var blockComplete: Boolean = false

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

          val prePrepareAction =
            if (shouldSendPrePrepare) {
              sentPrePrepare = true
              Seq(SendPbftMessage(pp, store = Some(StorePrePrepare(pp))))
            } else
              Seq.empty

          val prepareAction =
            if (shouldSendPrepare) {
              prePrepared = true
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
              myPrepare = Some(prepare)
              Seq(
                SendPbftMessage(
                  prepare,
                  store = Option.when(!isLeaderOfThisView)(StorePrePrepare(pp)),
                )
              )
            } else {
              Seq.empty
            }

          val commitAction =
            if (shouldSendCommit) {
              prepared = true
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
            } else {
              Seq.empty
            }

          val completeAction =
            if (shouldCompleteBlock) {
              committed = true
              Seq(CompletedBlock(pp, commitMessageQuorum, view))
            } else {
              Seq.empty
            }

          prePrepareAction ++ prepareAction ++ commitAction ++ completeAction
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

    // Send PrePrepare if I'm the original leader of a segment in view 0; future views disseminate
    // PrePrepares as part of the NewView message that completes a view change
    private def shouldSendPrePrepare: Boolean =
      view == ViewNumber.First && isLeaderOfThisView && prePrepare.isDefined && !sentPrePrepare

    private def shouldSendPrepare: Boolean =
      prePrepare.isDefined && !prePrepared

    private def shouldSendCommit(implicit traceContext: TraceContext): Boolean =
      prePrepare.fold(false) { pp =>
        val hash = pp.message.hash
        val (matchingHash, nonMatchingHash) = prepareMap.values.partition(_.message.hash == hash)
        val result =
          matchingHash.size >= membership.orderingTopology.strongQuorum && !prepared
        if (nonMatchingHash.nonEmpty)
          logger.warn(
            s"Found non-matching hashes for prepare messages from peers (${nonMatchingHash.map(_.from)})"
          )
        result
      }

    private def shouldCompleteBlock(implicit traceContext: TraceContext): Boolean =
      prePrepare.fold(false) { pp =>
        val hash = pp.message.hash
        val (matchingHash, nonMatchingHash) = commitMap.values.partition(_.message.hash == hash)
        val result =
          matchingHash.size >= membership.orderingTopology.strongQuorum && myCommit.isDefined && !committed
        if (nonMatchingHash.nonEmpty)
          logger.warn(
            s"Found non-matching hashes for commit messages from peers (${nonMatchingHash.map(_.from)})"
          )
        result
      }

    override def isBlockComplete: Boolean = blockComplete

    override def confirmCompleteBlockStored(): Unit =
      if (committed)
        blockComplete = true
      else
        abort("Uncommitted block shouldn't be stored")

    override def commitMessageQuorum: Seq[SignedMessage[Commit]] = {
      val hash =
        prePrepare
          .getOrElse(abort("The block is not complete (there is no PrePrepare)"))
          .message
          .hash
      if (committed) {
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
      } else if (prepared)
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
  }

  class AlreadyOrdered(
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
      true
    }
    override def advance()(implicit traceContext: TraceContext): Seq[ProcessResult] = Seq.empty
    override def isBlockComplete: Boolean = true
    override def confirmCompleteBlockStored(): Unit = ()
    override def commitMessageQuorum: Seq[SignedMessage[Commit]] = commitCertificate.commits
    override def consensusCertificate: Option[ConsensusCertificate] = Some(commitCertificate)
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

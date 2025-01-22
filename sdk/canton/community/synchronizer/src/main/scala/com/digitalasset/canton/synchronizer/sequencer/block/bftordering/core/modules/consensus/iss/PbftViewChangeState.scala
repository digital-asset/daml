// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import cats.instances.map.*
import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.ViewChangeMessageValidator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  NewView,
  PbftViewChangeEvent,
  PbftViewChangeMessage,
  PrePrepare,
  SignedPrePrepares,
  ViewChange,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

import IssConsensusModuleMetrics.emitNonCompliance

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.IterableOps"))
class PbftViewChangeState(
    membership: Membership,
    leader: SequencerId,
    epoch: EpochNumber,
    view: ViewNumber,
    blockNumbers: Seq[BlockNumber],
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit mc: MetricsContext)
    extends NamedLogging {
  private val messageValidator = new ViewChangeMessageValidator(membership, blockNumbers)
  private val viewChangeMap = mutable.HashMap[SequencerId, SignedMessage[ViewChange]]()
  private var viewChangeFromSelfWasFromRehydration = false
  private var signedPrePreparesForSegment: Option[Seq[SignedMessage[PrePrepare]]] = None
  private var newView: Option[SignedMessage[NewView]] = None

  def viewChangeMessageReceivedStatus: Seq[Boolean] =
    membership.sortedPeers.map(viewChangeMap.contains)

  /** Compute which view change messages we must retransmit based on which view change messages the remote node already has
    */
  def viewChangeMessagesToRetransmit(
      remoteNodeViewChangeMessages: Seq[Boolean]
  ): Seq[SignedMessage[ViewChange]] = {
    val messagesRemoteDoesNotHave =
      if (remoteNodeViewChangeMessages.isEmpty)
        // if they have nothing, we give them all the ones we have
        viewChangeMap.values.toSeq
      else
        // otherwise we give the ones we have that they don't have
        membership.sortedPeers
          .zip(remoteNodeViewChangeMessages)
          .flatMap { case (peer, hasIt) =>
            if (!hasIt) viewChangeMap.get(peer) else None
          }

    (messagesRemoteDoesNotHave
      .partition(_.from == membership.myId) match {
      case (fromSelf, fromOthers) =>
        // we put our own message first to make sure it gets included
        fromSelf ++ fromOthers
    }).take(
      // and we give only at most enough for them to complete a strong quorum
      membership.orderingTopology.strongQuorum - remoteNodeViewChangeMessages.count(identity)
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def processMessage(
      msg: SignedMessage[PbftViewChangeMessage]
  )(implicit traceContext: TraceContext): Boolean =
    msg.message match {
      case _: ViewChange =>
        addViewChange(msg.asInstanceOf[SignedMessage[ViewChange]])

      case _: NewView =>
        setNewView(msg.asInstanceOf[SignedMessage[NewView]])
    }

  def processEvent(abort: String => Unit)(
      event: PbftViewChangeEvent
  ): Boolean = event match {
    case SignedPrePrepares(_, viewNumber, signedMessages) =>
      setSignedPrePrepares(abort)(viewNumber, signedMessages)
  }

  def reachedWeakQuorum: Boolean = membership.orderingTopology.hasWeakQuorum(viewChangeMap.size)

  def shouldAdvanceViewChange: Boolean = {
    val enoughViewChangeMessagesFromPeersToStartViewChange = reachedWeakQuorum
    val hasReceivedNewViewMessage = newView.isDefined
    enoughViewChangeMessagesFromPeersToStartViewChange || hasReceivedNewViewMessage || viewChangeFromSelf.isDefined
  }

  def viewChangeFromSelf: Option[SignedMessage[ViewChange]] = viewChangeMap.get(membership.myId)
  def isViewChangeFromSelfRehydration: Boolean = viewChangeFromSelfWasFromRehydration
  def markViewChangeFromSelfasCommingFromRehydration(): Unit =
    viewChangeFromSelfWasFromRehydration = true

  def reachedStrongQuorum: Boolean = membership.orderingTopology.hasStrongQuorum(viewChangeMap.size)

  def shouldCreateNewView: Boolean =
    reachedStrongQuorum && newView.isEmpty && membership.myId == leader

  def shouldSendNewView: Boolean =
    newView.isDefined && membership.myId == leader

  def haveSignedPrePrepares: Boolean =
    signedPrePreparesForSegment.isDefined

  def getSignedPrePreparesForSegment: Option[Seq[SignedMessage[PrePrepare]]] =
    signedPrePreparesForSegment

  def constructPrePreparesForNewView(
      metadata: BlockMetadata,
      timestamp: CantonTimestamp,
  ): Seq[Either[PrePrepare, SignedMessage[PrePrepare]]] = {

    val viewChangeSet =
      viewChangeMap.values.toSeq.sortBy(_.from).take(membership.orderingTopology.strongQuorum)

    // Highest View-numbered PrePrepare from the vcSet defined for each block number
    val definedPrePrepares =
      NewView.computeCertificatePerBlock(viewChangeSet.map(_.message)).fmap(_.prePrepare)

    // Construct the final sequence of PrePrepares; use bottom block when no PrePrepare is defined
    blockNumbers.map { blockNum =>
      definedPrePrepares.get(blockNum) match {
        case Some(signedPrePrepare) => Right(signedPrePrepare)
        case None =>
          Left(
            PrePrepare.create(
              metadata.copy(blockNumber = blockNum),
              view,
              timestamp,
              OrderingBlock.empty,
              // We define the correct canonical commit set of bottom blocks as the empty set,
              //  because the previous ordered block from which to grab a canonical commit set is non-trivial
              //  to calculate, e.g., the previous block may itself be a bottom block.
              CanonicalCommitSet.empty,
              from = membership.myId,
            )
          )
      }
    }
  }

  def createNewViewMessage(
      metadata: BlockMetadata,
      segmentIdx: Int,
      timestamp: CantonTimestamp,
      prePrepares: Seq[SignedMessage[PrePrepare]],
  ): NewView = {

    // (Strong) quorum of validated view change messages collected from peers
    val viewChangeSet =
      viewChangeMap.values.toSeq.sortBy(_.from).take(membership.orderingTopology.strongQuorum)

    NewView.create(
      metadata,
      segmentIdx,
      view,
      timestamp,
      viewChangeSet,
      prePrepares,
      from = membership.myId,
    )
  }

  def newViewMessage: Option[SignedMessage[NewView]] = newView

  private def addViewChange(
      vc: SignedMessage[ViewChange]
  )(implicit traceContext: TraceContext): Boolean = {
    var stateChanged = false
    viewChangeMap.get(vc.from) match {
      case Some(_) =>
        logger.info(s"View change from ${vc.from} already exists; ignoring new vote")
      case None =>
        messageValidator.validateViewChangeMessage(vc.message) match {
          case Right(()) =>
            viewChangeMap.put(vc.from, vc).discard
            stateChanged = true
          case Left(error) =>
            emitNonCompliance(metrics)(
              vc.from,
              epoch,
              view,
              vc.message.blockMetadata.blockNumber,
              metrics.security.noncompliant.labels.violationType.values.ConsensusInvalidMessage,
            )
            logger.warn(
              s"Invalid view change message from ${vc.from}, ignoring vote. Reason: $error"
            )
        }
    }
    stateChanged
  }

  private def setNewView(
      nv: SignedMessage[NewView]
  )(implicit traceContext: TraceContext): Boolean = {
    var stateChange = false
    if (nv.from != leader) { // Ensure the message is from the current primary (leader) of the new view
      emitNonCompliance(metrics)(
        nv.from,
        epoch,
        view,
        nv.message.blockMetadata.blockNumber,
        metrics.security.noncompliant.labels.violationType.values.ConsensusRoleEquivocation,
      )
      logger.warn(s"New View message from ${nv.from}, but the leader of view $view is $leader")
    } else if (newView.isDefined) {
      logger.info(
        s"New view message for segment=${nv.message.segmentIndex} and view=$view already exists; ignoring new one from ${nv.from}"
      )
    } else {
      messageValidator.validateNewViewMessage(nv.message) match {
        case Right(()) =>
          newView = Some(nv)
          stateChange = true
        case Left(error) =>
          emitNonCompliance(metrics)(
            nv.from,
            epoch,
            view,
            nv.message.blockMetadata.blockNumber,
            metrics.security.noncompliant.labels.violationType.values.ConsensusInvalidMessage,
          )
          logger.warn(
            s"Invalid new view message from ${nv.from}, ignoring it. Reason: $error"
          )
      }
    }
    stateChange
  }

  private def setSignedPrePrepares(abort: String => Unit)(
      prePreparesViewNumber: ViewNumber,
      seq: Seq[SignedMessage[PrePrepare]],
  ): Boolean = {
    var stateChange = false
    if (prePreparesViewNumber != view) {
      abort(s"Signed PrePrepares from other view, ignoring $seq")
    } else if (signedPrePreparesForSegment.isDefined) {
      abort(s"Signed PrePrepares already exists, ignoring new one")
    } else {
      signedPrePreparesForSegment = Some(seq)
      stateChange = true
    }

    stateChange
  }
}

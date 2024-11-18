// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import cats.instances.map.*
import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.validation.ViewChangeMessageValidator
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  NewView,
  PbftViewChangeMessage,
  PrePrepare,
  ViewChange,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

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
  private var newView: Option[SignedMessage[NewView]] = None

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

  def reachedWeakQuorum: Boolean = membership.orderingTopology.hasWeakQuorum(viewChangeMap.size)

  def shouldAdvanceViewChange: Boolean = {
    val enoughViewChangeMessagesFromPeersToStartViewChange = reachedWeakQuorum
    val hasReceivedNewViewMessage = newView.isDefined
    enoughViewChangeMessagesFromPeersToStartViewChange || hasReceivedNewViewMessage || viewChangeFromSelf.isDefined
  }

  def viewChangeFromSelf: Option[SignedMessage[ViewChange]] = viewChangeMap.get(membership.myId)

  def reachedStrongQuorum: Boolean = membership.orderingTopology.hasStrongQuorum(viewChangeMap.size)

  def shouldCreateNewView: Boolean =
    reachedStrongQuorum && newView.isEmpty && membership.myId == leader

  def createNewViewMessage(
      metadata: BlockMetadata,
      segmentIdx: Int,
      timestamp: CantonTimestamp,
  ): SignedMessage[NewView] = {

    // (Strong) quorum of validated view change messages collected from peers
    val viewChangeSet =
      viewChangeMap.values.toSeq.sortBy(_.from).take(membership.orderingTopology.strongQuorum)

    // Highest View-numbered PrePrepare from the vcSet defined for each block number
    val definedPrePrepares =
      NewView.computeCertificatePerBlock(viewChangeSet.map(_.message)).fmap(_.prePrepare)

    // Construct the final sequence of PrePrepares; use bottom block when no PrePrepare is defined
    // TODO(#16820): Figure out what do ⊥ blocks look like exactly?
    val prePrepares = blockNumbers.map { blockNum =>
      definedPrePrepares.getOrElse(
        blockNum,
        SignedMessage(
          PrePrepare.create(
            blockMetadata = metadata.copy(blockNumber = blockNum),
            viewNumber = view,
            localTimestamp = timestamp,
            block = OrderingBlock(Seq.empty),
            // TODO(#16820): figure out what CanonicalCommitSet to use for bottom blocks
            canonicalCommitSet = CanonicalCommitSet(Set.empty),
            from = membership.myId,
          ),
          signature = Signature.noSignature, // TODO(#20458) actually sign this message
        ),
      )
    }

    val newViewMessage = SignedMessage(
      NewView.create(
        blockMetadata = metadata,
        segmentIndex = segmentIdx,
        viewNumber = view,
        localTimestamp = timestamp,
        viewChanges = viewChangeSet,
        prePrepares = prePrepares,
        from = membership.myId,
      ),
      Signature.noSignature,
    )

    newView = Some(newViewMessage)
    newViewMessage
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
        validateViewChangeMessage(vc.message) match {
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

  // TODO(#16820): add View Change validation logic
  // For each prepare cert in the View Change message:
  // - Validate signatures on each Prepare and PrePrepare TODO(i18194)
  // - Validate Prepares all have matching hash and are from enough distinct peers
  // - Is there some extra validation that should be done on the PrePrepare?
  //     - e.g., ensuring that blocks are ⊥ when they need to be
  private def validateViewChangeMessage(vc: ViewChange): Either[String, Unit] =
    messageValidator.validateViewChangeMessage(vc)

  private def setNewView(
      nv: SignedMessage[NewView]
  )(implicit traceContext: TraceContext): Boolean = {
    var stateChange = false
    if (nv.from != leader) {
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
      newView = Some(nv)
      stateChange = true
    }
    stateChange
  }

  // TODO(#16820): add New View validation logic
  // TODO(i18194) potentially check signatures
  // Ensure the message is from the current primary (leader) of the new view
  // Ensure there are enough View Change messages (strong quorum) from distinct peers
  // For each view change messages, call the validate view change function (above)
  private def isValidNewViewMessage(nv: NewView): Boolean = true
}

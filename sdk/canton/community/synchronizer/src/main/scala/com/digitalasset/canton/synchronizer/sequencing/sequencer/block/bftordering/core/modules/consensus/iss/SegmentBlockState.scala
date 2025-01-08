// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.PbftBlockState.{
  CompletedBlock,
  ProcessResult,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.ViewNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  NewViewStored,
  PbftMessagesStored,
  PbftNetworkMessage,
  PbftNormalCaseMessage,
  PrePrepareStored,
  PreparesStored,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusStatus
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class SegmentBlockState(
    factory: ViewNumber => PbftBlockState.InProgress,
    completedBlock: Option[Block],
) {
  private var currentViewNumber = ViewNumber.First
  private val views = new mutable.HashMap[ViewNumber, PbftBlockState.InProgress]()
  views(currentViewNumber) = factory(currentViewNumber)

  private var commitCertificate: Option[CommitCertificate] = None
  private var unconfirmedStorageCommitCertificate: Option[CommitCertificate] = None

  completedBlock.foreach { b =>
    commitCertificate = Some(b.commitCertificate)
  }

  def isComplete: Boolean = commitCertificate.isDefined

  def blockCommitMessages: Seq[SignedMessage[Commit]] =
    commitCertificate.toList.flatMap(_.commits)

  def completeBlock(cc: CommitCertificate): Seq[ProcessResult] =
    if (isComplete || unconfirmedStorageCommitCertificate.isDefined) Seq.empty
    else {
      unconfirmedStorageCommitCertificate = Some(cc)
      Seq(
        CompletedBlock(cc.prePrepare, cc.commits, currentViewNumber)
      )
    }

  def confirmCompleteBlockStored(viewNumber: ViewNumber): Unit =
    commitCertificate = unconfirmedStorageCommitCertificate.orElse {
      val block = views(viewNumber)
      block.confirmCompleteBlockStored()
      block.consensusCertificate.collect { case cc: CommitCertificate =>
        cc
      }
    }

  def advanceView(newViewNumber: ViewNumber): Unit = if (
    !isComplete && newViewNumber > currentViewNumber
  ) {
    currentViewNumber = newViewNumber
    views(currentViewNumber) = factory(currentViewNumber)
  }

  def consensusCertificate: Option[ConsensusCertificate] =
    // find the highest view from which there exists Some(ConsensusCertificate),
    // starting at newView-1 and moving all the way down to view=0, default to None if no such certificate exists
    // if the block has been completed, always just return the commit certificate
    commitCertificate.orElse(
      (ViewNumber.First to currentViewNumber).reverse
        .collectFirst(
          Function.unlift(viewNumber =>
            for {
              block <- views.get(ViewNumber(viewNumber))
              cert <- block.consensusCertificate
            } yield cert
          )
        )
    )

  def status(viewNumber: ViewNumber): ConsensusStatus.BlockStatus = if (isComplete)
    ConsensusStatus.BlockStatus.Complete
  else
    views.getOrElseUpdate(viewNumber, factory(viewNumber)).status

  def messagesToRetransmit(
      viewNumber: ViewNumber,
      fromStatus: ConsensusStatus.BlockStatus.InProgress,
  ): Seq[SignedMessage[PbftNetworkMessage]] =
    views.getOrElseUpdate(viewNumber, factory(viewNumber)).messagesToRetransmit(fromStatus)

  def processMessage(
      msg: SignedMessage[PbftNormalCaseMessage]
  )(implicit traceContext: TraceContext): Seq[ProcessResult] =
    if (isComplete) Seq.empty
    else if (views(currentViewNumber).processMessage(msg)) {
      views(currentViewNumber).advance()
    } else {
      Seq.empty
    }

  def processMessagesStored(pbftMessagesStored: PbftMessagesStored)(implicit
      traceContext: TraceContext
  ): Seq[ProcessResult] =
    if (!isComplete && pbftMessagesStored.viewNumber == currentViewNumber) {
      val block = views(currentViewNumber)
      pbftMessagesStored match {
        case _: PrePrepareStored =>
          block.confirmPrePrepareStored()
          block.advance()
        case _: PreparesStored =>
          block.confirmPreparesStored()
          block.advance()
        // if we're waiting for a commit certificate to have storage confirmed, we don't need to process NewViewStored
        // because in this case we would have skipped processing the pre-prepare from the NewView message for this block
        case _: NewViewStored if unconfirmedStorageCommitCertificate.isEmpty =>
          block.confirmPrePrepareStored()
          block.advance()
        case _ => Seq.empty
      }
    } else Seq.empty

  def prepareVoters: Iterable[SequencerId] = views.values.flatMap(_.prepareVoters)
  def commitVoters: Iterable[SequencerId] = views.values.flatMap(_.commitVoters)
}

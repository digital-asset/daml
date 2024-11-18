// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import cats.syntax.functor.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  NewView,
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
  ViewChange,
}

object SegmentInProgress {

  /** @return a tuple of lists, such that the first list are all prepares that should be restored
    *         across all views of the segment. There will be one quorum of prepare at most per block in this list,
    *         not in a specific order.
    *
    *         The other list is a list of messages that should be processed in order and are in increasing order
    *         of view number. First are all restored pre-prepares for the first view. Then all new-view messages
    *         for which there is at least one quorum of prepares in the same view.
    *         And then if there is a view-change message (indicating that it intended to start a view change),
    *         without a corresponding new-view message (indicating that the view change didn't finish), at the
    *         latest view, it is included in the end.
    *
    *         The reason prepares should be processed first in [[SegmentState]], is so that if as part of processing
    *         pre-prepares or new-views a prepare would be created that is already part of the rehydration messages,
    *         the rehydrated ones would be taken instead of creating new ones.
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def rehydrationMessages(
      segment: Segment,
      epochInProgress: EpochStore.EpochInProgress,
  ): (Seq[SignedMessage[Prepare]], Seq[SignedMessage[PbftNetworkMessage]]) = {
    val completedBlocks = epochInProgress.completedBlocks.map(_.blockNumber)
    val isSegmentComplete =
      segment.slotNumbers.forall(blockNumber => completedBlocks.contains(blockNumber))

    if (isSegmentComplete) (Seq.empty, Seq.empty)
    else {
      val segmentInProgressMessages = epochInProgress.pbftMessagesForIncompleteBlocks
        .filter { msg =>
          val blockNumber = msg.message.blockMetadata.blockNumber
          segment.slotNumbers.contains(blockNumber)
        }
      val highestView =
        segmentInProgressMessages.map(_.message.viewNumber).maxOption.getOrElse(ViewNumber.First)

      // for each block, we only care about the quorum of prepares for the highest view
      // since it will either be the one currently being worked on or
      // the one that would be picked in case of a view change (prepare certificate)
      // obs: since we always store prepares only when reaching a quorum, we always have complete quorums here
      val preparesPerView: Map[ViewNumber, List[SignedMessage[Prepare]]] = {
        val prepareQuorumPerBlock: Map[BlockNumber, Seq[SignedMessage[Prepare]]] =
          segmentInProgressMessages
            .collect { case s @ SignedMessage(_: Prepare, _) =>
              s.asInstanceOf[SignedMessage[Prepare]]
            }
            .groupBy(_.message.blockMetadata.blockNumber)
            .fmap { allPreparesForBlock =>
              val viewNumberOfPrepareCertificate =
                allPreparesForBlock.map(_.message.viewNumber).maxOption.getOrElse(ViewNumber.First)
              allPreparesForBlock.filter(_.message.viewNumber == viewNumberOfPrepareCertificate)
            }
        // take all the highest quorums per blocks and let's group them by view number
        prepareQuorumPerBlock.values.flatten.toList.groupBy(_.message.viewNumber)
      }

      // we only care about the pre-prepares for the initial view,
      // since any other pre-prepares would be included in new-view messages
      val initialPrePreparesPerBlock: List[SignedMessage[PrePrepare]] = segmentInProgressMessages
        .collect { case s @ SignedMessage(_: PrePrepare, _) =>
          s.asInstanceOf[SignedMessage[PrePrepare]]
        }
        .groupBy(_.message.blockMetadata.blockNumber)
        .fmap { allPrePreparesForBlock =>
          allPrePreparesForBlock.collectFirst {
            case s @ SignedMessage(pp: PrePrepare, _) if pp.viewNumber == ViewNumber.First =>
              s
          }
        }
        .values
        .flatten
        .toList

      // to restore a working state, we need the successfully completed view changes (new-view messages),
      // but we only need the latest one and the ones where there is at least one prepare quorum
      val newViews: Seq[SignedMessage[NewView]] =
        segmentInProgressMessages
          .collect {
            case s @ SignedMessage(nv: NewView, _)
                if (nv.viewNumber == highestView) || preparesPerView.contains(nv.viewNumber) =>
              s.asInstanceOf[SignedMessage[NewView]]
          }
          .sortBy(_.message.viewNumber)

      // we only care about this node's view change message if it is for the latest view and there is
      // no new-view message at that view. before that, the new-view messages are enough.
      val viewChangeAtLatestView: Option[SignedMessage[ViewChange]] = {
        val highestNewViewViewNumber = segmentInProgressMessages
          .collect { case s @ SignedMessage(newView: NewView, _) => newView.viewNumber }
          .maxOption
          .getOrElse(ViewNumber.First)
        segmentInProgressMessages
          .collectFirst {
            case s @ SignedMessage(vc: ViewChange, _)
                if vc.viewNumber == highestView && vc.viewNumber > highestNewViewViewNumber =>
              s.asInstanceOf[SignedMessage[ViewChange]]
          }
      }

      (
        preparesPerView.values.flatten.toList,
        (initialPrePreparesPerBlock: Seq[
          SignedMessage[PbftNetworkMessage]
        ]) ++ newViews ++ viewChangeAtLatestView.toList,
      )
    }
  }
}

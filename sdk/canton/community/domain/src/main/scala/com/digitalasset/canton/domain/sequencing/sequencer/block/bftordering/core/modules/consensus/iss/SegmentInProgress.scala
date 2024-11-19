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
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  NewView,
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
  ViewChange,
}

object SegmentInProgress {

  /** This object contains the organized information needed for the crash-recovery of a segment.
    *
    * @param prepares contains all prepares that should be restored across all views of the segment.
    *                 In this list there will be at most one quorum of prepares per block, without a specific order.
    *                 Prepares should be processed first in [[SegmentState]]; in this way, if a prepare is created,
    *                 as part of processing pre-prepares or new-views, that is already part of the rehydration messages,
    *                 the rehydrated ones would be taken instead of creating new ones.
    * @param oldViewsMessages is a list of messages that should be processed in order and are in increasing order
    *                         of view number, for all views before the latest one.
    *                         All restored pre-prepares for the first view appear first. Then all new-view messages
    *                         for which there is at least one quorum of prepares in the same view.
    * @param currentViewMessages is similar to the previous one, but for the current view only.
    *                            If at the latest view there is a view-change message (indicating the intent to start a view change),
    *                            without a corresponding new-view message (i.e., the view change didn't finish),
    *                            the view-change message will be the only message in this collection.
    */
  final case class RehydrationMessages(
      prepares: Seq[SignedMessage[Prepare]],
      oldViewsMessages: Seq[SignedMessage[PbftNetworkMessage]],
      currentViewMessages: Seq[SignedMessage[PbftNetworkMessage]],
  ) {

    private lazy val preparesStores
        : Map[ViewNumber, Map[BlockMetadata, ConsensusMessage.PreparesStored]] =
      prepares
        .groupBy(_.message.viewNumber)
        .map { case (viewNumber, prepares) =>
          viewNumber -> prepares.groupBy(_.message.blockMetadata).map { case (blockMetadata, _) =>
            blockMetadata -> ConsensusSegment.ConsensusMessage
              .PreparesStored(blockMetadata, viewNumber)
          }
        }

    def preparesStoredForViewNumber(viewNumber: ViewNumber): Seq[ConsensusMessage.PreparesStored] =
      for {
        map <- preparesStores.get(viewNumber).toList
        stored <- map.values
      } yield stored

    def preparesStoredForBlockAtViewNumber(
        blockMetadata: BlockMetadata,
        viewNumber: ViewNumber,
    ): Option[ConsensusMessage.PreparesStored] =
      preparesStores
        .get(viewNumber)
        .flatMap(_.get(blockMetadata))
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def rehydrationMessages(
      segment: Segment,
      epochInProgress: EpochStore.EpochInProgress,
  ): RehydrationMessages = {
    val isSegmentComplete = {
      val completedBlocks = epochInProgress.completedBlocks.map(_.blockNumber)
      segment.slotNumbers.forall(blockNumber => completedBlocks.contains(blockNumber))
    }

    if (isSegmentComplete) RehydrationMessages(Seq.empty, Seq.empty, Seq.empty)
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
          .collect { case SignedMessage(newView: NewView, _) => newView.viewNumber }
          .maxOption
          .getOrElse(ViewNumber.First)
        segmentInProgressMessages
          .collectFirst {
            case s @ SignedMessage(vc: ViewChange, _)
                if vc.viewNumber == highestView && vc.viewNumber > highestNewViewViewNumber =>
              s.asInstanceOf[SignedMessage[ViewChange]]
          }
      }

      val messages = (initialPrePreparesPerBlock: Seq[
        SignedMessage[PbftNetworkMessage]
      ]) ++ newViews ++ viewChangeAtLatestView.toList
      RehydrationMessages(
        prepares = preparesPerView.values.flatten.toList,
        oldViewsMessages = messages.filter(_.message.viewNumber < highestView),
        currentViewMessages = messages.filter(_.message.viewNumber == highestView),
      )
    }
  }
}

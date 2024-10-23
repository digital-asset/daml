// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.memory

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.{
  Block,
  Epoch,
  EpochInProgress,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.GenericInMemoryEpochStore.{
  CompletedBlock,
  EpochStatus,
  pbftEventSortData,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  Genesis,
  OrderedBlocksReader,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
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
  NewView,
  PbftEvent,
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
  ViewChange,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.MapView
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Success, Try}

abstract class GenericInMemoryEpochStore[E <: Env[E]]
    extends EpochStore[E]
    with OrderedBlocksReader[E] {
  private val epochs: TrieMap[EpochNumber, EpochStatus] = TrieMap.empty
  private val blocks: TrieMap[BlockNumber, CompletedBlock] = TrieMap.empty

  // The maps below are for messages for in-progress blocks.
  private val prePreparesMap: TrieMap[BlockNumber, TrieMap[ViewNumber, PrePrepare]] = TrieMap.empty
  private val preparesMap: TrieMap[BlockNumber, TrieMap[ViewNumber, Seq[Prepare]]] = TrieMap.empty
  private val viewChangesMap: TrieMap[BlockNumber, TrieMap[ViewNumber, ViewChange]] = TrieMap.empty
  private val newViewsMap: TrieMap[BlockNumber, TrieMap[ViewNumber, NewView]] = TrieMap.empty

  protected def createFuture[T](action: String)(value: () => Try[T]): E#FutureUnlessShutdownT[T]

  private def addSingleMessageToMap[M <: PbftNetworkMessage](
      storeType: String,
      map: TrieMap[BlockNumber, TrieMap[ViewNumber, M]],
  )(
      message: M
  ): Try[Unit] = {
    map
      .putIfAbsent(
        message.blockMetadata.blockNumber,
        new TrieMap[ViewNumber, M](),
      )
      .discard
    putIfAbsent(
      store = map(message.blockMetadata.blockNumber),
      key = message.viewNumber,
      value = message,
      storeType = s"$storeType(${message.blockMetadata.blockNumber})",
    )
  }
  private def putIfAbsent[K, V](
      store: TrieMap[K, V],
      key: K,
      value: V,
      storeType: String,
  ): Try[Unit] = store.putIfAbsent(key, value) match {
    case None =>
      Success(())
    case Some(v) =>
      if (v == value) Success(())
      else
        Failure(
          new RuntimeException(
            s"Updating existing entry in $storeType is illegal: key:$key, oldValue: $v, newValue: $value"
          )
        )
  }

  override def startEpoch(
      epoch: EpochInfo
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit] =
    createFuture(startEpochActionName(epoch)) { () =>
      putIfAbsent(
        store = epochs,
        key = epoch.number,
        value = EpochStatus(epoch, isInProgress = true),
        storeType = "epochs",
      )
    }

  override def completeEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit] =
    createFuture(completeEpochActionName(epochNumber)) { () =>
      // delete all in-progress messages after an epoch ends and before we start adding new messages in the new epoch
      prePreparesMap.clear()
      preparesMap.clear()
      viewChangesMap.clear()
      newViewsMap.clear()
      Success(
        epochs
          .updateWith(epochNumber) {
            case Some(EpochStatus(epoch, _)) => Some(EpochStatus(epoch, isInProgress = false))
            case None => None
          }
          .discard
      )
    }

  override def latestEpoch(includeInProgress: Boolean)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[EpochStore.Epoch] = createFuture(latestCompletedEpochActionName) {
    () =>
      Try {
        val epochInfo = epochs
          .filter { case (_, EpochStatus(_, isInProgress)) =>
            includeInProgress || (!includeInProgress && !isInProgress)
          }
          .maxByOption { case (epochNumber, _) => epochNumber }
          .map { case (_, EpochStatus(epochInfo, _)) => epochInfo }
          .getOrElse(Genesis.GenesisEpochInfo)
        val commits =
          blocks.get(epochInfo.lastBlockNumber).fold[Seq[Commit]](Seq.empty)(_.commits)
        Epoch(epochInfo, commits)
      }
  }

  override def addPrePrepare(
      prePrepare: PrePrepare
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit] =
    createFuture(addPrePrepareActionName(prePrepare)) { () =>
      addSingleMessageToMap("pre-prepares", prePreparesMap)(prePrepare)
    }

  override def addPrepares(
      prepares: Seq[Prepare]
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit] =
    createFuture(addPreparesActionName) { () =>
      prepares.headOption.fold[Try[Unit]](Success(())) { head =>
        preparesMap
          .putIfAbsent(head.blockMetadata.blockNumber, TrieMap[ViewNumber, Seq[Prepare]]())
          .discard
        putIfAbsent(
          store = preparesMap(head.blockMetadata.blockNumber),
          key = head.viewNumber,
          value = prepares,
          storeType = s"prepares(${head.blockMetadata.blockNumber})",
        )
      }
    }

  override def addViewChangeMessage[M <: ConsensusMessage.PbftViewChangeMessage](
      viewChangeMessage: M
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Unit] =
    createFuture(addViewChangeMessageActionName(viewChangeMessage)) { () =>
      viewChangeMessage match {
        case vc: ViewChange =>
          addSingleMessageToMap("view-changes", viewChangesMap)(vc)
        case nv: NewView =>
          addSingleMessageToMap("new-views", newViewsMap)(nv)
      }
    }

  override def addOrderedBlock(
      prePrepare: PrePrepare,
      commitMessages: Seq[Commit],
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Unit] = {
    val epochNumber = prePrepare.blockMetadata.epochNumber
    val blockNumber = prePrepare.blockMetadata.blockNumber
    createFuture(addOrderedBlockActionName(epochNumber, blockNumber)) { () =>
      // we can drop the in progress messages for this block
      preparesMap.remove(blockNumber).discard
      prePreparesMap.remove(blockNumber).discard

      putIfAbsent(
        store = blocks,
        key = blockNumber,
        value = CompletedBlock(prePrepare, commitMessages),
        storeType = "orderedBlocks",
      )
    }
  }

  override def loadEpochProgress(activeEpochInfo: EpochInfo)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[EpochInProgress] =
    createFuture(loadEpochProgressActionName(activeEpochInfo)) { () =>
      Try {
        val blocksInEpoch = blocks.view
          .filter { case (_, CompletedBlock(prePrepare, _)) =>
            prePrepare.blockMetadata.epochNumber == activeEpochInfo.number
          }
          .values
          .toSeq
          .map { case CompletedBlock(prePrepare, commits) =>
            Block(
              prePrepare.blockMetadata.epochNumber,
              prePrepare.blockMetadata.blockNumber,
              CommitCertificate(prePrepare, commits),
            )
          }
          .sortWith(_.blockNumber < _.blockNumber)

        val preparesForIncompleteBlocks = preparesMap.view
          .filter { case (blockNumber, _) =>
            blockNumber >= activeEpochInfo.startBlockNumber
          }
          .values
          .flatMap(_.values)
          .flatten
          .toList
          .sortBy(pbftEventSortData)

        def messagesForIncompleteBlocks[M <: PbftNetworkMessage](
            mapView: MapView[BlockNumber, TrieMap[ViewNumber, M]]
        ): List[PbftNetworkMessage] = mapView
          .filter { case (blockNumber, _) =>
            blockNumber >= activeEpochInfo.startBlockNumber
          }
          .values
          .flatMap(_.values)
          .toList
          .sortBy(pbftEventSortData)

        val prePreparesForIncompleteBlocks = messagesForIncompleteBlocks(prePreparesMap.view)
        val viewChangesForIncompleteSegments = messagesForIncompleteBlocks(viewChangesMap.view)
        val newViewsForIncompleteSegments = messagesForIncompleteBlocks(newViewsMap.view)

        val pbftMessagesForIncompleteBlocks =
          viewChangesForIncompleteSegments ++ newViewsForIncompleteSegments ++
            (prePreparesForIncompleteBlocks: List[
              PbftNetworkMessage
            ]) ++ preparesForIncompleteBlocks
        EpochInProgress(blocksInEpoch, pbftMessagesForIncompleteBlocks)
      }
    }

  override def loadPrePreparesForCompleteBlocks(
      startEpochNumberInclusive: EpochNumber,
      endEpochNumberInclusive: EpochNumber,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Seq[PrePrepare]] =
    createFuture(loadPrePreparesActionName(startEpochNumberInclusive, endEpochNumberInclusive)) {
      () =>
        val prePrepares = blocks.view
          .filter { case (_, CompletedBlock(prePrepare, _)) =>
            val epochNumber = prePrepare.blockMetadata.epochNumber
            epochNumber >= startEpochNumberInclusive && epochNumber <= endEpochNumberInclusive
          }
          .values
          .map(_.prePrepare)
          .toSeq
        Success(prePrepares)
    }

  override def loadOrderedBlocks(
      initialBlockNumber: BlockNumber
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Seq[OrderedBlockForOutput]] =
    createFuture(loadOrderedBlocksActionName(initialBlockNumber)) { () =>
      blocks.view
        .filter { case (blockNumber, _) =>
          blockNumber >= initialBlockNumber
        }
        .values
        .foldLeft[Try[Seq[OrderedBlockForOutput]]](Success(Seq.empty)) {
          case (partialResult, next) =>
            partialResult.flatMap { orderedBlocks =>
              val CompletedBlock(prePrepare, _) = next
              val epochNumber = prePrepare.blockMetadata.epochNumber
              val blockNumber = prePrepare.blockMetadata.blockNumber
              epochs.get(epochNumber) match {
                case Some(EpochStatus(epochInfo, _)) =>
                  val isBlockLastInEpoch =
                    epochInfo.lastBlockNumber == blockNumber
                  Success(
                    orderedBlocks :+
                      OrderedBlockForOutput(
                        OrderedBlock(
                          prePrepare.blockMetadata,
                          prePrepare.block.proofs,
                          prePrepare.canonicalCommitSet,
                        ),
                        prePrepare.from,
                        isBlockLastInEpoch,
                      )
                  )
                case None =>
                  Failure(
                    new RuntimeException(
                      s"Epoch $epochNumber not found for block $blockNumber"
                    )
                  )
              }
            }
        }
    }
}

private object GenericInMemoryEpochStore {

  final case class EpochStatus(epochInfo: EpochInfo, isInProgress: Boolean)

  final case class CompletedBlock(prePrepare: PrePrepare, commits: Seq[Commit])

  def pbftEventSortData(pbftEvent: PbftEvent): (BlockNumber, ViewNumber) =
    (pbftEvent.blockMetadata.blockNumber, pbftEvent.viewNumber)
}

final class InMemoryEpochStore extends GenericInMemoryEpochStore[PekkoEnv] {
  override protected def createFuture[T](action: String)(
      value: () => Try[T]
  ): PekkoFutureUnlessShutdown[T] =
    PekkoFutureUnlessShutdown(action, FutureUnlessShutdown.fromTry(value()))
}

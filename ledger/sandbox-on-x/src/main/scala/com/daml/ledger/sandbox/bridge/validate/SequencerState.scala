// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.offset.Offset
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.bridge.validate.SequencerState.{LastUpdatedAt, SequencerQueue}
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.ContractId
import com.daml.metrics.api.MetricsContext.withEmptyMetricsContext

import scala.collection.Searching
import scala.util.chaining._

/** State of the sequencing conflict checking stage
  *
  * @param sequencerQueue A queue ordered by each transaction's offset.
  * @param keyState Mapping of all key updates occurred over the span of the `sequencerQueue` coupled
  *                 with the transaction offset that last updated them.
  * @param consumedContractsState Map of all consumed contracts over the span of the `sequencerQueue`.
  */
case class SequencerState private (
    private[validate] val sequencerQueue: SequencerQueue,
    private[validate] val keyState: Map[GlobalKey, (Option[ContractId], LastUpdatedAt)],
    private[validate] val consumedContractsState: Set[ContractId],
)(implicit bridgeMetrics: BridgeMetrics) {

  def enqueue(
      offset: Offset,
      updatedKeys: Map[GlobalKey, Option[ContractId]],
      consumedContracts: Set[ContractId],
  ): SequencerState =
    if (sequencerQueue.lastOption.exists(_._1 >= offset))
      throw new RuntimeException(
        s"Offset to be enqueued ($offset) is not higher than the last enqueued offset (${sequencerQueue.last._1})"
      )
    else
      new SequencerState(
        sequencerQueue = sequencerQueue :+ (offset -> (updatedKeys, consumedContracts)),
        keyState = keyState ++ updatedKeys.view.mapValues(_ -> offset),
        consumedContractsState = consumedContractsState ++ consumedContracts,
      ).tap(updateMetrics)

  def dequeue(noConflictUpTo: Offset): SequencerState = {
    val pruneBefore = sequencerQueue.view.map(_._1).search(noConflictUpTo) match {
      case Searching.Found(foundIndex) => foundIndex + 1
      case Searching.InsertionPoint(insertionPoint) => insertionPoint
    }

    val (evictedQueueEntries, prunedQueue) = sequencerQueue.splitAt(pruneBefore)

    val keyStateEntriesToBeEvicted =
      evictedQueueEntries.iterator
        .flatMap { case (_, (updatedKeys, _)) => updatedKeys.keySet }
        .filter { key =>
          val (_, lastUpdatedAt) = keyState(key)
          lastUpdatedAt <= noConflictUpTo
        }
        .toSet

    val prunedKeyState = keyState -- keyStateEntriesToBeEvicted

    val prunedConsumedContractsState =
      consumedContractsState.diff(evictedQueueEntries.iterator.flatMap(_._2._2).toSet)

    new SequencerState(
      sequencerQueue = prunedQueue,
      keyState = prunedKeyState,
      consumedContractsState = prunedConsumedContractsState,
    )
  }

  private def updateMetrics(newState: SequencerState): Unit = withEmptyMetricsContext {
    implicit metricsContext =>
      val stateMetrics = bridgeMetrics.Stages.Sequence
      stateMetrics.sequencerQueueLength.update(newState.sequencerQueue.length)
      stateMetrics.keyStateSize.update(newState.keyState.size)
      stateMetrics.consumedContractsStateSize.update(newState.consumedContractsState.size)
  }
}

object SequencerState {
  // Override default apply to prevent construction of an already-populated state.
  def apply(
      sequencerQueue: SequencerQueue,
      keyState: Map[GlobalKey, (Option[ContractId], LastUpdatedAt)],
      consumedContractsState: Set[ContractId],
  )(implicit bridgeMetrics: BridgeMetrics): SequencerState = {
    require(sequencerQueue.isEmpty, "The sequencer state queue must be empty at initialization")
    require(keyState.isEmpty, "The sequencer updated keys state must be empty at initialization")
    require(
      consumedContractsState.isEmpty,
      "The sequencer consumed contracts state must be empty at initialization",
    )

    new SequencerState(sequencerQueue, keyState, consumedContractsState)
  }

  private[validate] def empty(implicit bridgeMetrics: BridgeMetrics) =
    new SequencerState(
      sequencerQueue = Vector.empty,
      keyState = Map.empty,
      consumedContractsState = Set.empty,
    )
  private[sandbox] type LastUpdatedAt = Offset
  private[sandbox] type SequencerQueue =
    Vector[(Offset, (Map[GlobalKey, Option[ContractId]], Set[ContractId]))]
}

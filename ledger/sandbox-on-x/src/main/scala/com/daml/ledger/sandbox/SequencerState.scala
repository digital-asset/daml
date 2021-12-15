// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package ledger.sandbox

import ledger.offset.Offset
import ledger.sandbox.SequencerState.{LastUpdatedAt, SequencerQueue}
import lf.transaction.GlobalKey
import platform.store.appendonlydao.events.{ContractId, Key}

import scala.collection.Searching
import scala.util.chaining._

/** State of the sequencing conflict checking stage
  *
  * @param sequencerQueue A queue ordered by each transaction's `noConflictUpTo`
  * @param keyState Mapping of all key updates occurred over the span of the `sequencerQueue` coupled
  *                 with the transaction offset that last updated them.
  * @param consumedContractsState Map of all consumed contracts over the span of the `sequencerQueue`..
  */
case class SequencerState(
    sequencerQueue: SequencerQueue = Vector.empty,
    keyState: Map[Key, (Option[ContractId], LastUpdatedAt)] = Map.empty,
    consumedContractsState: Set[ContractId] = Set.empty,
)(bridgeMetrics: BridgeMetrics) {

  def enqueue(
      offset: Offset,
      updatedKeys: Map[Key, Option[ContractId]],
      consumedContracts: Set[ContractId],
  ): SequencerState =
    if (sequencerQueue.lastOption.exists(_._1 > offset))
      throw new RuntimeException(
        s"Offset to be enqueued ($offset) is smaller than the last enqueued offset (${sequencerQueue.last._1})"
      )
    else
      SequencerState(
        sequencerQueue = sequencerQueue :+ (offset -> (updatedKeys, consumedContracts)),
        keyState = keyState ++ updatedKeys.view.mapValues(_ -> offset),
        consumedContractsState = consumedContractsState ++ consumedContracts,
      )(bridgeMetrics)
        .tap(newState => {
          bridgeMetrics.SequencerState.sequencerQueueLength
            .update(newState.sequencerQueue.length)
          bridgeMetrics.SequencerState.keyStateSize.update(newState.keyState.size)
          bridgeMetrics.SequencerState.consumedContractsStateSize
            .update(newState.consumedContractsState.size)
        })

  def dequeue(upToOffset: Offset): SequencerState = {
    val pruneAfter = sequencerQueue.view.map(_._1).search(upToOffset) match {
      case Searching.Found(foundIndex) => foundIndex + 1
      case Searching.InsertionPoint(insertionPoint) => insertionPoint
    }

    val (evictedEntries, prunedQueue) = sequencerQueue.splitAt(pruneAfter)

    val prunedKeyState =
      evictedEntries.iterator
        .flatMap { case (onConflictUpTo, (updatedKeys, _)) =>
          updatedKeys.iterator.map { case (key, _) => onConflictUpTo -> key }
        }
        .foldLeft(keyState) { case (updatedKeyState, (noConflictUpTo, key)) =>
          updatedKeyState.get(key).fold(throw new RuntimeException("Should not be missing")) {
            case (_, offset) if noConflictUpTo == offset => updatedKeyState - key
            case _ => updatedKeyState
          }
        }

    val prunedConsumedContractsState =
      consumedContractsState.diff(evictedEntries.iterator.flatMap(_._2._2).toSet)

    SequencerState(
      sequencerQueue = prunedQueue,
      keyState = prunedKeyState,
      consumedContractsState = prunedConsumedContractsState,
    )(bridgeMetrics)
  }
}

object SequencerState {
  private[sandbox] type LastUpdatedAt = Offset
  private[sandbox] type SequencerQueue =
    Vector[(Offset, (Map[GlobalKey, Option[ContractId]], Set[ContractId]))]
}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.data.{Counter, PeanoTreeQueue}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber

class PeanoQueue[T](initialHead: BlockNumber) {
  type BlockNumberCounterDiscriminator

  private val BlockNumberCounter = Counter[BlockNumberCounterDiscriminator]

  private val peanoQueue: PeanoTreeQueue[BlockNumberCounterDiscriminator, T] =
    new PeanoTreeQueue[BlockNumberCounterDiscriminator, T](
      BlockNumberCounter(initialHead)
    )

  def head: Counter[BlockNumberCounterDiscriminator] = peanoQueue.head

  def alreadyInserted(key: BlockNumber): Boolean =
    peanoQueue.alreadyInserted(BlockNumberCounter(key))

  def insertAndPoll(blockNumber: BlockNumber, item: T): Seq[T] = {
    peanoQueue.insert(BlockNumberCounter(blockNumber), item).discard
    LazyList
      .continually(peanoQueue.poll())
      .takeWhile(_.isDefined)
      .flatMap {
        case Some(_ -> blockData) => LazyList(blockData)
        case None => LazyList.empty
      }
      .toList
  }
}

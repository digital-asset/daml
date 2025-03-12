// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.data
import com.digitalasset.canton.data.{Counter, PeanoTreeQueue}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber

class PeanoQueue[T](initialHead: BlockNumber)(abort: String => Nothing) {
  type BlockNumberCounterDiscriminator

  private val BlockNumberCounter = Counter[BlockNumberCounterDiscriminator]

  private val peanoQueue: PeanoTreeQueue[BlockNumberCounterDiscriminator, T] =
    new PeanoTreeQueue[BlockNumberCounterDiscriminator, T](
      BlockNumberCounter(initialHead)
    )

  def head: Counter[BlockNumberCounterDiscriminator] = peanoQueue.head

  def headValue: Option[T] =
    peanoQueue.get(head) match {
      case data.PeanoQueue.NotInserted(_, _) => None
      case data.PeanoQueue.InsertedValue(value) => Some(value)
      case data.PeanoQueue.BeforeHead =>
        abort(s"Head $head is before the actual head ${peanoQueue.head}")
    }

  def alreadyInserted(key: BlockNumber): Boolean =
    peanoQueue.alreadyInserted(BlockNumberCounter(key))

  def insert(key: BlockNumber, value: T): Unit =
    peanoQueue.insert(BlockNumberCounter(key), value).discard

  def pollAvailable(pollWhile: Option[T] => Boolean = _.isDefined): Seq[T] =
    LazyList
      .continually {
        if (pollWhile(headValue))
          peanoQueue.poll()
        else
          None
      }
      .takeWhile(_.isDefined)
      .flatMap(_.map(_._2))
      .toList
}

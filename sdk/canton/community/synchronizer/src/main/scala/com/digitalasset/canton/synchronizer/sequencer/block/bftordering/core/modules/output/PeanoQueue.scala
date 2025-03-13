// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.data
import com.digitalasset.canton.data.{Counter, PeanoTreeQueue}
import com.digitalasset.canton.discard.Implicits.DiscardOps

class PeanoQueue[KeyT <: Long, ValueT](initialHead: KeyT)(
    abort: String => Nothing
) {

  type PeanoQueueCounterDiscriminator

  private val PeanoQueueCounter = Counter[PeanoQueueCounterDiscriminator]

  private val peanoQueue: PeanoTreeQueue[PeanoQueueCounterDiscriminator, ValueT] =
    new PeanoTreeQueue[PeanoQueueCounterDiscriminator, ValueT](
      PeanoQueueCounter(initialHead)
    )

  def head: Counter[PeanoQueueCounterDiscriminator] = peanoQueue.head

  def headValue: Option[ValueT] =
    peanoQueue.get(head) match {
      case data.PeanoQueue.NotInserted(_, _) => None
      case data.PeanoQueue.InsertedValue(value) => Some(value)
      case data.PeanoQueue.BeforeHead =>
        abort(s"Head $head is before the actual head ${peanoQueue.head}")
    }

  def alreadyInserted(key: KeyT): Boolean =
    peanoQueue.alreadyInserted(PeanoQueueCounter(key))

  def insert(key: KeyT, value: ValueT): Unit =
    peanoQueue.insert(PeanoQueueCounter(key), value).discard

  def pollAvailable(pollWhile: Option[ValueT] => Boolean = _.isDefined): Seq[ValueT] =
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

// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.control

import com.daml.ledger.javaapi.data.Party
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.Mutex

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.mutable

class Balancer {

  class Item(val party: Party) extends Ordered[Item] {
    val outstanding = new AtomicInteger(0)
    val counter = new AtomicInteger(0)
    override def compare(that: Item): Int = this.outstanding.get().compare(that.outstanding.get())
    def update(change: Int): Int = {
      counter.incrementAndGet()
      outstanding.updateAndGet(x => Math.max(0, x + change))
    }
  }
  private val ordered = mutable.ArrayBuffer[Item]()
  private val lock = new Mutex()

  /** extend queue with new members */
  def updateMembers(current: Seq[Party]): Unit = (lock.exclusive {
    val members = ordered.map(_.party).toSet
    current.filter(!members.contains(_)).foreach { party =>
      val itm = new Item(party)
      ordered.prepend(itm)
    }
    require(
      ordered.sizeIs == current.size,
      s"Failed to update members. Expected: ${current.length}, actual: ${ordered.length}.",
    )
  })

  @tailrec
  private def swapIfNecessary(direction: Int, ii: Int): Unit = {
    val (mm, nn) = if (direction == 1) (ii, ii + 1) else (ii - 1, ii)
    if (mm > -1 && nn < ordered.length) {
      if (ordered(mm).outstanding.get() > ordered(nn).outstanding.get()) {
        val tmp = ordered(mm)
        ordered(mm) = ordered(nn)
        ordered(nn) = tmp
        swapIfNecessary(direction, ii + direction)
      }
    }
  }

  @tailrec
  private def update(ii: Int, party: Party, change: Int): Unit = {
    require(change == 1 || change == -1, s"Illegal value for change: $change.")
    val cur = ordered(ii)
    // if this is our party row
    if (cur.party == party) {
      // update outstanding and check if we need to swap
      cur.update(change).discard[Int]
      // we changed, so ensure we are in order again
      swapIfNecessary(change, ii)
    } else if (ii < ordered.length - 1) {
      update(ii + 1, party, change)
    } else {
      updateMembers(Seq(party))
      update(0, party, change)
    }
  }

  def completed(party: Party): Unit = (lock.exclusive {
    update(0, party, -1)
  })

  /** manually adjust stats on startup */
  def adjust(party: Party, count: Int): Unit = (lock.exclusive {
    update(0, party, count)
  })

  def state(): Seq[(Party, Int, Int)] = (lock.exclusive {
    ordered.map(x => (x.party, x.outstanding.get(), x.counter.get())).toSeq
  })

  def next(): Party = (lock.exclusive {
    require(ordered.nonEmpty, "calling next on empty balancer will not work")
    val head = ordered(0).party
    update(0, head, 1)
    head
  })

}

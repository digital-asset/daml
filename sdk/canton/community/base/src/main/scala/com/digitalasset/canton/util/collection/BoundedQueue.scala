// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.collection

import com.digitalasset.canton.discard.Implicits.DiscardOps

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.While"))
class BoundedQueue[A](maxQueueSize: Int) extends mutable.Queue[A] {

  override def addOne(elem: A): BoundedQueue.this.type = {
    val ret = super.addOne(elem)
    trim()
    ret
  }

  override def addAll(elems: IterableOnce[A]): BoundedQueue.this.type = {
    val ret = super.addAll(elems)
    trim()
    ret
  }

  private def trim(): Unit =
    while (sizeIs > maxQueueSize) dequeue().discard
}

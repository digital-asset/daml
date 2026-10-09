// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.collection

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.collection.BoundedQueue.DropStrategy

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.While"))
class BoundedQueue[A](maxQueueSize: Int, dropStrategy: DropStrategy = DropStrategy.DropOldest)
    extends mutable.Queue[A] {

  override def addOne(elem: A): this.type = {
    val ret = super.addOne(elem)
    trim()
    ret
  }

  override def addAll(elems: IterableOnce[A]): this.type = {
    val ret = super.addAll(elems)
    trim()
    ret
  }

  private def trim(): Unit =
    while (sizeIs > maxQueueSize) {
      dropStrategy match {
        case DropStrategy.DropOldest =>
          removeHeadOption().discard
        case DropStrategy.DropNewest =>
          removeLastOption().discard
      }
    }
}

object BoundedQueue {
  sealed trait DropStrategy extends Product with Serializable
  object DropStrategy {
    case object DropOldest extends DropStrategy
    case object DropNewest extends DropStrategy
  }
}

// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.logging.pretty.Pretty

import java.util.concurrent.atomic.AtomicReference

/** A mutable list to which elements can be prepended and where snapshots can be taken atomically.
  * Both operations are constant-time.
  * Thread safe.
  */
class SnapshottableList[A] {

  private val list: AtomicReference[List[A]] = new AtomicReference[List[A]](Nil)

  /** Obtains a snapshot of the current list.
    * Subsequent modifications of this list do not show up in the snapshot.
    */
  def snapshot: List[A] = list.get()

  /** Prepends an element to the list */
  def add(x: A): Unit = {
    val _ = list.getAndUpdate(tail => x :: tail)
  }

  override def toString: String = list.get.toString
}

object SnapshottableList {
  def empty[A]: SnapshottableList[A] = new SnapshottableList[A]

  def prettySnapshottableList[A: Pretty]: Pretty[SnapshottableList[A]] = {
    import Pretty.*
    prettyOfParam(_.snapshot)
  }
}

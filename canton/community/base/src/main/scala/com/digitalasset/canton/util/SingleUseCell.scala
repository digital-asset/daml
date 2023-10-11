// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import java.util.concurrent.atomic.AtomicReference

/** This class provides a mutable container for a single value of type `A`.
  * The value may be put at most once. A [[SingleUseCell]] therefore provides the following immutability guarantee:
  * The value of a cell cannot change; once it has been put there, it will remain in the cell.
  */
class SingleUseCell[A] {
  private val content: AtomicReference[Option[A]] = new AtomicReference[Option[A]](None)

  /** Returns whether the value has not yet been set */
  def isEmpty: Boolean = content.get.isEmpty

  /** Inserts the given value into the cell if it was empty before.
    * Otherwise returns the content.
    *
    * @return The previous value or [[scala.None$]] if the cell was empty.
    */
  def putIfAbsent(x: A): Option[A] =
    if (content.compareAndSet(None, Some(x))) None else content.get()

  /** Returns the contents of the cell, if any. */
  def get: Option[A] = content.get

  /** Shorthand for `get.getOrElse` */
  def getOrElse[B >: A](x: => B): B = content.get.getOrElse(x)

  override def toString: String = content.get match {
    case None => "Cell(<empty>)"
    case Some(x) => "Cell(" + x.toString + ")"
  }
}

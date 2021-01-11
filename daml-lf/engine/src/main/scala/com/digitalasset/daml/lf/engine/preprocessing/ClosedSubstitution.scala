// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import data.Ref

// To handle a closed substitution in a lazy way while traversing a term top down.
private[preprocessing] class ClosedSubstitution[X] private (
    mapping: Map[Ref.Name, (ClosedSubstitution[X], X)]
) {

  def get(name: Ref.Name): Option[(ClosedSubstitution[X], X)] =
    mapping.get(name)

  def introVar(name: Ref.Name, x: X) =
    new ClosedSubstitution(mapping.updated(name, this -> x))

  // requirement: names.size == xs.size
  def introVars(names: Iterable[Ref.Name], xs: Iterable[X]): ClosedSubstitution[X] = {
    if (names.isEmpty && xs.isEmpty) {
      this
    } else {
      val namesIter = names.iterator
      val xsIter = xs.iterator
      var acc = mapping
      while (namesIter.hasNext && namesIter.hasNext) {
        acc = acc.updated(namesIter.next(), this -> xsIter.next())
      }
      require(namesIter.isEmpty && namesIter.isEmpty)
      new ClosedSubstitution(acc)
    }
  }
}

object ClosedSubstitution {
  private val EmptySingleton = new ClosedSubstitution[Nothing](Map.empty)

  def empty[X]: ClosedSubstitution[X] =
    EmptySingleton.asInstanceOf[ClosedSubstitution[X]]
}

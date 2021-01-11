// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.language.Ast.{TVar, Type}
import data.Ref

// To handle a closed substitution in a lazy way while traversing a term top down.
private[preprocessing] class DelayedTypeSubstitution private(
    mapping: Map[Ref.Name, (DelayedTypeSubstitution, Type)]
) {

  // substitute typ if type is a variable
  // always return a non variable type
  @throws[IllegalArgumentException]
  def apply(typ: Type): (DelayedTypeSubstitution, Type) =
    typ match {
      case TVar(name) =>
        mapping.getOrElse(name, throw new IllegalArgumentException(s"unexpected free variable $name"))
      case otherwise => (this -> otherwise)
    }

  // variables that appear in `typ` must be in the domain of `mapping`
  def introVar(name: Ref.Name, typ: Type) =
    new DelayedTypeSubstitution(mapping.updated(name, this -> apply(typ)))

  // variables that appear in `typ` in the domain of `mapping`
  // requirement: names.size == xs.size
  def introVars(names: Iterable[Ref.Name], typs: Iterable[Type]): DelayedTypeSubstitution = {
    if (names.isEmpty && typs.isEmpty) {
      this
    } else {
      val namesIter = names.iterator
      val typsIter = typs.iterator
      var acc = mapping
      while (namesIter.hasNext && typsIter.hasNext) {
        acc = acc.updated(namesIter.next(), this -> apply(typsIter.next))
      }
      require(namesIter.isEmpty && namesIter.isEmpty)
      new DelayedTypeSubstitution(acc)
    }
  }
}

object DelayedTypeSubstitution {
 val Empty = new DelayedTypeSubstitution(Map.empty)
}

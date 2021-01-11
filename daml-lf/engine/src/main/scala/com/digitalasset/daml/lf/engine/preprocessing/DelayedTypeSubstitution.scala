// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.language.Ast.{TVar, Type}
import data.Ref

// To handle a closed substitution in a lazy way while traversing a term top down.
private[preprocessing] class DelayedTypeSubstitution private (
    mapping: Map[Ref.Name, (DelayedTypeSubstitution, Type)]
) {

  // substitute typ if type is a variable
  // always return a non variable type
  @throws[IllegalArgumentException]
  def apply(typ: Type): (DelayedTypeSubstitution, Type) =
    typ match {
      case TVar(name) =>
        mapping.getOrElse(
          name,
          throw new IllegalArgumentException(s"unexpected free variable $name"),
        )
      case otherwise => (this -> otherwise)
    }

  // variables that appear in the co-domain of `newMapping` must be in the domain of `mapping`
  def introVars(newMapping: Iterable[(Ref.Name, Type)]): DelayedTypeSubstitution = {
    if (newMapping.isEmpty) {
      this
    } else {
      val updatedMapping = newMapping.foldLeft(mapping) { case (acc, (name, typ)) =>
        acc.updated(name, apply(typ))
      }
      new DelayedTypeSubstitution(updatedMapping)
    }
  }
}

object DelayedTypeSubstitution {
  val Empty = new DelayedTypeSubstitution(Map.empty)
}

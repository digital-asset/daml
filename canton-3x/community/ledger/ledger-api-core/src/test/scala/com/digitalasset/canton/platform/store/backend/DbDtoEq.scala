// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import org.scalactic.Equality
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

// DbDto case classes contain serialized values in Arrays (sometimes wrapped in Options),
// because this representation can efficiently be passed to Jdbc.
// Using Arrays means DbDto instances are not comparable, so we have to define a custom equality operator.
object DbDtoEq extends Matchers {

  @nowarn("cat=lint-infer-any")
  val DbDtoEq: org.scalactic.Equality[DbDto] = {
    case (a: DbDto, b: DbDto) =>
      (a.productPrefix === b.productPrefix) &&
      (a.productArity == b.productArity) &&
      (a.productIterator zip b.productIterator).forall {
        case (x: Array[_], y: Array[_]) => x sameElements y
        case (Some(x: Array[_]), Some(y: Array[_])) => x sameElements y
        case (x, y) => x === y
      }
    case (_, _) => false
  }

  val DbDtoSeqEq: org.scalactic.Equality[Seq[DbDto]] = {
    case (a: Seq[_], b: Seq[_]) =>
      a.size == b.size && a.zip(b).forall({ case (x, y) => DbDtoEq.areEqual(x, y) })
    case (_, _) => false
  }

  implicit val eqOptArray: Equality[Option[Array[Byte]]] = (first: Option[Array[Byte]], b: Any) => {
    val second = Option(b).getOrElse(Some[Array[Byte]]).asInstanceOf[Option[Array[Byte]]]
    (first, second) match {
      case (None, None) => true
      case (None, Some(s)) => s.isEmpty
      case (Some(f), None) => f.isEmpty
      case (Some(f), Some(s)) => f === s
    }
  }

}

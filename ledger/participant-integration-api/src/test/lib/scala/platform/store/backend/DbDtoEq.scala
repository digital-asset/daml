// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.matchers.should.Matchers

// DbDto case classes contain serialized values in Arrays (sometimes wrapped in Options),
// because this representation can efficiently be passed to Jdbc.
// Using Arrays means DbDto instances are not comparable, so we have to define a custom equality operator.
object DbDtoEq extends Matchers {

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

}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.softwaremill.diffx._
import scalaz.{@@, Tag}

trait DiffExtensions {

  implicit def diffTag[A, T](implicit diffA: Diff[A]): Diff[A @@ T] = {
    new Diff[A @@ T] {
      override def apply(left: A @@ T, right: A @@ T, toIgnore: List[FieldPath]): DiffResult = {
        diffA.apply(Tag.unwrap(left), Tag.unwrap(right))
      }

    }
  }

}

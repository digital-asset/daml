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

  implicit def diffSeq[T](implicit diffT: Diff[T]): Diff[Seq[T]] =
    new Diff[Seq[T]] {
      override def apply(left: Seq[T], right: Seq[T], toIgnore: List[FieldPath]): DiffResult = {
        val changed = left.toStream
          .zip(right.toStream)
          .map { case (l, r) => diffT.apply(l, r) }
          .collect { case result if !result.isIdentical => result }

        val removed: Seq[DiffResult] = left.toStream
          .drop(right.length)
          .map(DiffResultMissing.apply)
        val added: Seq[DiffResult] = right.toStream
          .drop(left.length)
          .map(DiffResultAdditional.apply)

        assert(
          removed.isEmpty || added.isEmpty,
          "Diff[Seq[_]] thinks that both sequences are longer than each other.",
        )

        val differences = changed ++ removed ++ added

        if (differences.isEmpty) Identical(left)
        else DiffResultString(differences.toList)
      }
    }

}

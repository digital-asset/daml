// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import ai.x.diff.{Comparison, DiffShow, Different, Identical, green, red}
import com.softwaremill.diffx
//import com.softwaremill.diffx.generic.auto._
import com.softwaremill.diffx._
import scalaz.{@@, Tag}

trait DiffExtensions {
//  implicit def diffShowTag[A, T](implicit diffShowA: DiffShow[A]): DiffShow[A @@ T] =
//    new DiffShow[A @@ T] {
//      override def show(t: A @@ T): String = diffShowA.show(Tag.unwrap(t))
//
//      override def diff(left: A @@ T, right: A @@ T): Comparison =
//        diffShowA.diff(Tag.unwrap(left), Tag.unwrap(right))
//    }

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
          .map(diffx.DiffResultMissing.apply)
        val added: Seq[DiffResult] = right.toStream
          .drop(left.length)
          .map(diffx.DiffResultAdditional.apply)

        assert(
          removed.isEmpty || added.isEmpty,
          "Diff[Seq[_]] thinks that both sequences are longer than each other.",
        )

        val differences = changed ++ removed ++ added

        if (differences.isEmpty) diffx.Identical(left)
        else diffx.DiffResultString(differences.toList)
      }
    }

  implicit def diffShowSeq[T](implicit diffShowT: DiffShow[T]): DiffShow[Seq[T]] =
    new DiffShow[Seq[T]] {
      override def show(t: Seq[T]): String = t.toString

      override def diff(left: Seq[T], right: Seq[T]): Comparison = {
        val changed = left.toStream
          .zip(right.toStream)
          .zipWithIndex
          .map { case ((l, r), index) => index -> diffShowT.diff(l, r) }
          .collect { case (index, diff) if !diff.isIdentical => index.toString -> diff.string }
        val removed = left.toStream.zipWithIndex.drop(right.length).map { case (value, index) =>
          index.toString -> red(diffShowT.show(value))
        }
        val added = right.toStream.zipWithIndex.drop(left.length).map { case (value, index) =>
          index.toString -> green(diffShowT.show(value))
        }

        assert(
          removed.isEmpty || added.isEmpty,
          "Diff[Seq[_]] thinks that both sequences are longer than each other.",
        )

        if (changed.isEmpty && removed.isEmpty && added.isEmpty) {
          Identical(left)
        } else {
          val changedOption =
            if (changed.isEmpty)
              Stream(None)
            else
              changed.map(Some(_))
          val differences = changedOption ++ removed.map(Some(_)) ++ added.map(Some(_))
          Different(DiffShow.constructorOption("Seq", differences.toList))
        }
      }
    }
}

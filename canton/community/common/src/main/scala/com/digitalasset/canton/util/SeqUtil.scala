// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}

import scala.annotation.tailrec

object SeqUtil {

  /** Splits the sequence `xs` after each element that satisfies `p` and returns the sequence of chunks */
  def splitAfter[A](xs: Seq[A])(p: A => Boolean): Seq[NonEmpty[Seq[A]]] = {
    @tailrec def go(acc: Seq[NonEmpty[Seq[A]]], rest: Seq[A]): Seq[NonEmpty[Seq[A]]] = {
      val (before, next) = rest.span(!p(_))
      NonEmpty.from(next) match {
        case None => NonEmpty.from(before).fold(acc)(_ +: acc)
        case Some(nextNE) =>
          go(NonEmptyUtil.fromUnsafe(before :+ nextNE.head1) +: acc, nextNE.tail1)
      }
    }
    go(Seq.empty, xs).reverse
  }

  /** Groups consecutive elements of the input list `xs` if they have the same `key` */
  def clusterBy[A, K](xs: Seq[A])(key: A => K): Seq[NonEmpty[Seq[A]]] = {
    val grouped = Seq.newBuilder[NonEmpty[Seq[A]]]

    @tailrec def go(remaining: Seq[A]): Unit = {
      NonEmpty.from(remaining) match {
        case Some(remainingNE) =>
          val k = key(remainingNE.head1)
          val (cluster, rest) = remainingNE.span(x => k == key(x))
          grouped.addOne(NonEmptyUtil.fromUnsafe(cluster))
          go(rest)
        case None => ()
      }
    }

    go(xs)
    grouped.result()
  }
}

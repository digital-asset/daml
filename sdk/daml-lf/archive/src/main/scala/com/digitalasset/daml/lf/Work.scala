// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf

import scala.collection.SeqView

// stack-safety achieved via a Work trampoline.
private sealed abstract class Work[A]

private object Work {
  final case class Ret[A](v: A) extends Work[A]

  final case class Delay[A](thunk: () => Work[A]) extends Work[A]

  final case class Bind[A, X](work: Work[X], k: X => Work[A]) extends Work[A]

  def run[R](work: Work[R]): R = {
    // calls to run Work must never be nested
    @scala.annotation.tailrec
    def loop[A](work: Work[A]): A = work match {
      case Ret(v) => v
      case Delay(thunk) => loop(thunk())
      case Bind(work, k) =>
        loop(work match {
          case Ret(x) => k(x)
          case Delay(thunk) => Bind(thunk(), k)
          case Bind(work1, k1) => Bind(work1, ((x: Any) => Bind(k1(x), k)))
        })
    }

    loop(work)
  }

  def bind[A, X](work: Work[X])(k: X => Work[A]): Work[A] =
    Work.Bind(work, k)

  def sequence[A, B](works: SeqView[Work[A]])(k: List[A] => Work[B]): Work[B] = {
    def loop(acc: List[A], works: List[Work[A]]): Work[B] = {
      works match {
        case Nil => k(acc.reverse)
        case work :: works =>
          bind(work) { x =>
            loop(x :: acc, works)
          }
      }
    }

    loop(Nil, works.toList)
  }
}

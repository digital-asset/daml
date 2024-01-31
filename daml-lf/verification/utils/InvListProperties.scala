// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import stainless.collection._
import stainless.lang._
import stainless.annotation._

object ListProperties {

  extension[T](l: List[T]) {
    def bindexOf(e: T): BigInt = l.indexOf(e)
    def blength: BigInt = l.length
    def bapply(i: BigInt): T = {
      require(i >= 0)
      require(i < l.length)
      l(i)
    }
  }

  @pure @opaque
  def notForallWitness[T](l: List[T], f: T => Boolean): T = {
    decreases(l)
    require(!l.forall(f))
    l match {
      case Nil() => Unreachable()
      case Cons(h, t) => if (!f(h)) h else notForallWitness(t, f)
    }
  }.ensuring(res => l.contains(res) && !f(res))

  @pure
  @opaque
  def concatContains[T](@induct l1: List[T], l2: List[T], e: T): Unit = {}.ensuring(
    (l1 ++ l2).contains(e) == (l1.contains(e) || l2.contains(e))
  )

  @pure
  @opaque
  def forallConcat[T](l1: List[T], l2: List[T], p: T => Boolean): Unit = {

    if ((l1 ++ l2).forall(p)) {
      if (!l1.forall(p)) {
        val w = notForallWitness(l1, p)
        concatContains(l1, l2, w)
        forallContains(l1 ++ l2, p, w)
      }
      if (!l2.forall(p)) {
        val w = notForallWitness(l2, p)
        concatContains(l1, l2, w)
        forallContains(l1 ++ l2, p, w)
      }
    }
    if (l1.forall(p) && l2.forall(p) && !(l1 ++ l2).forall(p)) {
      val w = notForallWitness(l1 ++ l2, p)
      concatContains(l1, l2, w)
      if (l1.contains(w)) forallContains(l1, p, w) else forallContains(l2, p, w)
    }
  }.ensuring((l1 ++ l2).forall(p) == (l1.forall(p) && l2.forall(p)))

  @pure @opaque
  def forallContains[T](l: List[T], f: T => Boolean, e: T): Unit = {
    if (l.forall(f) && l.contains(e)) {
      ListSpecs.forallContained(l, f, e)
    }
  }.ensuring((l.forall(f) && l.contains(e)) ==> f(e))

  @pure
  @opaque
  def bapplyContains[T](tr: List[T], i: BigInt): Unit = {
    decreases(tr)
    require(i >= 0)
    require(i < tr.blength)
    tr match {
      case Nil() => Trivial()
      case Cons(h, t) =>
        if (i == 0) {
          Trivial()
        } else {
          bapplyContains(t, i - 1)
        }
    }
  }.ensuring(tr.contains(tr.bapply(i)))

  def isUnique[T](tr: List[T]): Boolean = {
    decreases(tr)
    tr match {
      case Nil() => true
      case Cons(h, t) => !t.contains(h) && isUnique(t)
    }
  }

  @pure
  @opaque
  def isUniqueIndex[T](tr: List[T], i1: BigInt, i2: BigInt): Unit = {
    require(i1 >= 0)
    require(i2 >= 0)
    require(i1 < tr.blength)
    require(i2 < tr.blength)
    require(isUnique(tr))
    decreases(tr)
    tr match {
      case Nil() => Trivial()
      case Cons(h, t) =>
        if ((i1 == 0) && (i2 == 0)) {
          Trivial()
        } else if (i1 == 0) {
          bapplyContains(t, i2 - 1)
        } else if (i2 == 0) {
          bapplyContains(t, i1 - 1)
        } else {
          isUniqueIndex(t, i1 - 1, i2 - 1)
        }
    }
  }.ensuring((tr.bapply(i1) == tr.bapply(i2)) == (i1 == i2))

  @pure @opaque
  def bapplyBindexOf[T](l: List[T], e: T): Unit = {
    decreases(l)
    require(l.contains(e))
    l match {
      case Nil() => Unreachable()
      case Cons(h, t) =>
        if (h == e) {
          Trivial()
        } else {
          bapplyBindexOf(t, e)
        }
    }
  }.ensuring(
    l.bapply(l.bindexOf(e)) == e
  )

  @pure
  @opaque
  def bindexOfLast[T](l: List[T], e: T): Unit = {
    require(l.bindexOf(e) >= l.blength - 1)
    require(!l.isEmpty)
    decreases(l)
    require(l.contains(e))
    l match {
      case Nil() => Unreachable()
      case Cons(h, t) =>
        if (t.isEmpty) {
          Trivial()
        } else {
          bindexOfLast(t, e)
        }
    }
  }.ensuring(
    e == l.last
  )

  @pure
  def next[T](l: List[T], e: T): T = {
    require(l.contains(e))
    require(e != l.last)
    if (l.bindexOf(e) >= l.blength - 1) {
      bindexOfLast(l, e)
    }
    l.bapply(l.bindexOf(e) + 1)
  }.ensuring(l.contains)

  @pure
  @opaque
  def concatIndex[T](l1: List[T], l2: List[T], i: BigInt): Unit = {
    decreases(l1)
    require(i >= 0)
    require(i < l1.size + l2.size)
    l1 match {
      case Nil() => Trivial()
      case Cons(h1, t1) =>
        if (i == 0) {
          Trivial()
        } else {
          concatIndex(t1, l2, i - 1)
        }
    }
  }.ensuring(
    (l1 ++ l2)(i) == (if (i < l1.size) l1(i) else l2(i - l1.size))
  )

}

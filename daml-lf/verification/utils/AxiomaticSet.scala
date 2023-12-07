// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import stainless.lang._
import stainless.annotation._
import scala.collection.{Set => ScalaSet}

import SetProperties._

case class Set[T](@pure @extern toScala: ScalaSet[T]) {

  import SetAxioms._

  @pure @opaque
  def isEmpty: Boolean = this === Set.empty[T]

  @pure @extern
  def size: BigInt = toScala.size

  @pure @opaque
  def contains: T => Boolean = e => exists(_ == e)

  @pure @alias
  def apply(e: T): Boolean = contains(e)

  @pure @extern
  def forall(f: T => Boolean): Boolean = toScala.forall(f)

  @pure @extern
  def exists(f: T => Boolean): Boolean = toScala.exists(f)

  @pure @opaque
  def subsetOf(s2: Set[T]): Boolean = forall(s2.contains)

  @pure @extern
  def union(s2: Set[T]): Set[T] = Set(toScala ++ s2.toScala)
  @pure @alias
  def ++(s2: Set[T]): Set[T] = union(s2)

  @pure @opaque
  def incl(e: T): Set[T] = {
    val res = union(Set(e))

    @pure @opaque
    def inclProp: Unit = {
      unionContains(this, Set(e), e)
      singletonContains(e)
      subsetOfUnion(this, Set(e))
      singletonSize(e)
      if (contains(e)) {
        intersectSingleton(this, e)
        unionSize(this, Set(e))
        sizeEquals(intersect(Set(e)), Set(e))
      } else {
        disjointSingleton(this, e)
        unionDisjointSize(this, Set(e))
      }

      if (res.isEmpty) {
        isEmptyContains(res, e)
      }
    }.ensuring(
      res.contains(e) &&
        subsetOf(res) &&
        (res.size == (if (contains(e)) size else size + 1)) &&
        !res.isEmpty
    )
    inclProp
    res
  }.ensuring { res =>
    res.contains(e) &&
    subsetOf(res) &&
    (res.size == (if (contains(e)) size else size + 1)) &&
    !res.isEmpty
  }

  @pure @alias
  def +(e: T): Set[T] = incl(e)

  @pure @extern
  def filter(f: T => Boolean): Set[T] = Set(toScala.filter(f))

  @pure
  @extern
  def map[V](f: T => V): Set[V] = Set(toScala.map(f))

  @pure
  @opaque
  def diff(s2: Set[T]): Set[T] = {
    filterSubsetOf(this, x => !s2.contains(x))
    filter(!s2.contains(_))
  }.ensuring(res => res.subsetOf(this))

  @pure @alias
  def &~(s2: Set[T]): Set[T] = diff(s2)

  @pure @opaque
  def remove(e: T): Set[T] = {
    diff(Set[T](e))
  }.ensuring(res => res.subsetOf(this))

  @pure @alias
  def -(e: T): Set[T] = remove(e)

  @pure
  @opaque
  def symDiff(s2: Set[T]): Set[T] = diff(s2) ++ s2.diff(this)

  @pure @opaque
  def intersect(s2: Set[T]): Set[T] = union(s2) &~ symDiff(s2)

  @pure @opaque
  def disjoint(s2: Set[T]): Boolean = intersect(s2).isEmpty

  @pure @alias
  def &(s2: Set[T]): Set[T] = intersect(s2)

  @pure @extern
  def witness(f: T => Boolean): T = {
    require(exists(f))
    ??? : T
  }.ensuring(w => contains(w) && f(w))

  /** Extensional equality for finite sets
    *
    * Should not be opaque in order to have automatic symmetry
    */
  @pure @nopaque
  def ===(s2: Set[T]): Boolean = subsetOf(s2) && s2.subsetOf(this)

  @pure @alias
  def =/=(s2: Set[T]): Boolean = !(this === s2)

  @pure @opaque
  def find(p: T => Boolean): Option[T] = {
    ??? : Option[T]
  }.ensuring(res =>
    res match {
      case None[T]() => !exists(p)
      case Some[T](x) => contains(x) && p(x)
    }
  )
}

object Set {

  @pure @extern
  def empty[T]: Set[T] = Set[T](ScalaSet[T]())

  @pure @extern
  def apply[T](e: T): Set[T] = Set[T](ScalaSet[T](e))

  @pure
  def range(a: BigInt, b: BigInt): Set[BigInt] = {
    decreases(b - a)
    require(a <= b)
    if (a == b) {
      Set.empty[BigInt]
    } else {
      Set.range(a + 1, b) + a
    }
  }

}

object SetAxioms {

  /** Size nonnegativity axiom
    *
    * The size of any set is always non negative
    */
  @pure
  @extern
  def sizePositive[T](s: Set[T]): Unit = {}.ensuring(s.size >= BigInt(0))

  /** Singleton size axiom
    *
    * The size of a singleton is 1
    */
  @pure @extern
  def singletonSize[T](e: T): Unit = {}.ensuring(Set(e).size == BigInt(1))

  /** Disjoint union size axiom
    *
    * The size of a disjoint union is the sum of the sizes of the set
    */
  @pure
  @extern
  def unionDisjointSize[T](s1: Set[T], s2: Set[T]): Unit = {
    require(s1.disjoint(s2))
  }.ensuring((s1 ++ s2).size == s1.size + s2.size)

  /** Congruence size axiom
    *
    * If two sets are equal then their size is also equal
    */
  @pure
  @extern
  def sizeEquals[T](s1: Set[T], s2: Set[T]): Unit = {
    require(s1 === s2)
  }.ensuring(s1.size == s2.size)

  /** De Morgan's laws for quantifiers
    *
    * The second one should not be an axiom if we assume that !!f == f
    * which Stainless is not able to prove.
    */
  @pure @extern @inlineOnce
  def notForallExists[T](s: Set[T], f: T => Boolean): Unit = {}.ensuring(
    !s.forall(f) == s.exists(!f(_))
  )

  @pure
  @extern
  @inlineOnce
  def forallNotExists[T](s: Set[T], f: T => Boolean): Unit = {}.ensuring(
    !s.forall(!f(_)) == s.exists(f)
  )

  /** Existential quantifier definition
    *
    * Should not be an axiom if we assume some properties on lambdas that
    * Stainless is not able to prove
    */
  @pure @extern
  def witnessExists[T](s: Set[T], f: T => Boolean, w: T): Unit = {
    require(s.contains(w))
    require(f(w))
  }.ensuring(s.exists(f))

  /** Axiom of empty set
    *
    * Empty introduction axiom: any predicate on the empty set is valid. Equivalent
    * to the ZF empty set axiom stating that there exists an empty such that it
    * contains no element.
    */
  @pure
  @extern
  def forallEmpty[T](f: T => Boolean): Unit = {}.ensuring(Set.empty.forall(f))

  /** Axiom of union
    *
    * Union introduction axiom: any predicate is true on the union of two sets if
    * and only if it is true for both.
    */
  @pure @extern
  def forallUnion[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {}.ensuring(
    (s1 ++ s2).forall(f) == (s1.forall(f) && s2.forall(f))
  )

  /** Axiom of filter
    *
    * Filter introduction axiom: if a set is filtered by a predicate then all the element
    * of the set verify this predicate.
    */
  @pure
  @extern
  @inlineOnce
  def forallFilter[T](s: Set[T], f: T => Boolean, p: T => Boolean): Unit = {}.ensuring(
    s.filter(f).forall(p) == s.forall(x => f(x) ==> p(x))
  )

  /** Axiom of map
    *
    * Map introduction axiom: if a set is filtered by a predicate then all the element
    * of the set verify this predicate.
    */
  @pure
  @extern
  @inlineOnce
  def forallMap[T, V](s: Set[T], f: T => V, p: V => Boolean): Unit = {}.ensuring(
    s.map(f).forall(p) == s.forall(f andThen p)
  )

  /** Axiom of singleton
    *
    * Singleton introduction axiom: a predicate is true on a singleton if and only if
    * it is valid for its unique element. Equivalent to an axiom for incl.
    */
  @pure @extern
  def forallSingleton[T](e: T, f: T => Boolean): Unit = {}.ensuring(Set(e).forall(f) == f(e))

  /** Extensionality axiom
    *
    * If two sets are subset of each other then their are equal
    */
  @pure
  @extern
  def extensionality[T](s1: Set[T], s2: Set[T]): Unit = {
    require(s1 === s2)
  }.ensuring(s1 == s2)

}

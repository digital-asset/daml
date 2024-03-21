// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import stainless.lang.{Set => StainlessSet, BooleanDecorations, unfold}
import stainless.lang
import stainless.annotation._
import SetAxioms._

/** ∀ ∅ ⊆ ∈ ∪ ∃
  */

object SetProperties {

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------EMPTY------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** The empty set is a subset of any set.
    *
    *  - ∀s. ∅ ⊆ s
    */
  @pure
  @opaque
  def emptySubsetOf[T](s: Set[T]): Unit = {
    unfold(Set.empty.subsetOf(s))
    forallEmpty(s.contains)
  }.ensuring(Set.empty.subsetOf(s))

  /** The empty set has size 0.
    *
    *  - |∅| = 0
    */
  @pure
  @opaque
  def emptySize[T]: Unit = {
    disjointEmpty(Set.empty[T])
    unionEmpty(Set.empty[T])
    sizeEquals(Set.empty[T] ++ Set.empty[T], Set.empty[T])
    unionDisjointSize(Set.empty[T], Set.empty[T])
    sizePositive(Set.empty[T])
  }.ensuring(Set.empty[T].size == BigInt(0))

  /** There is no element satisfying a given predicate in the empty set.
    *
    *  - ∀p. (!∃x ∈ ∅. p(x))
    */
  @pure
  @opaque
  def emptyExists[T](p: T => Boolean): Unit = {
    forallNotExists(Set.empty, p)
    forallEmpty[T](!p(_))
  }.ensuring(!Set.empty.exists(p))

  /** Adding an element to the empty set gives the singleton of that element.
    */
  @pure
  @opaque
  def inclEmpty[T](e: T): Unit = {
    unfold(Set.empty.incl(e))
    unionEmpty(Set[T](e))
    equalsReflexivity(Set(e))
  }.ensuring(Set.empty.incl(e) === Set(e))

  /** The empty set does not contain any element.
    *
    *  - ∀x. x ∉ ∅
    */
  @pure
  @opaque
  def emptyContains[T](e: T): Unit = {
    if (Set.empty(e)) {
      singletonSubsetOf(Set.empty, e)
      val neq: T => Boolean = _ != e
      forallEmpty(neq)
      forallSingleton(e, neq)
      forallSubsetOf(Set(e), Set.empty, neq)
    }
  }.ensuring(!Set.empty(e))

  /** The intersection with the empty set is the empty set (i.e. the empty set is an absorbing element).
    *
    *  - ∀s. s ∩ ∅ = ∅
    *  - ∀s. ∅ ∩ s = ∅
    */
  @pure @opaque
  def intersectionEmpty[T](s: Set[T]): Unit = {
    intersectAltDefinition(Set.empty[T], s)
    emptySubsetOf(s)
    intersectCommutativity(s, Set.empty[T])
    equalsTransitivity(s & Set.empty[T], Set.empty[T] & s, Set.empty[T])
  }.ensuring(
    ((Set.empty[T] & s) === Set.empty[T]) &&
      ((s & Set.empty[T]) === Set.empty[T])
  )

  /** Any set is disjoint with the empty set.
    */
  @pure
  @opaque
  def disjointEmpty[T](s: Set[T]): Unit = {
    intersectionEmpty(s)
    unfold(Set.empty[T].disjoint(s))
    unfold((Set.empty[T] & s).isEmpty)
    disjointSymmetry(Set.empty[T], s)
  }.ensuring(Set.empty[T].disjoint(s) && s.disjoint(Set.empty[T]))

  /** The empty set is a neutral element with respect to the union.
    *
    *  - ∀s. s ∪ ∅ = ∅
    *  - ∀s. ∅ ∪ s = ∅
    */
  @pure
  @opaque
  def unionEmpty[T](s: Set[T]): Unit = {
    unionAltDefinition(Set.empty[T], s)
    emptySubsetOf(s)
    unionCommutativity(s, Set.empty[T])
    equalsTransitivity(s ++ Set.empty[T], Set.empty[T] ++ s, s)
  }.ensuring(
    s ++ Set.empty[T] === s &&
      Set.empty[T] ++ s === s
  )

  /** The image of the empty set with respect to a function is also the empty set.
    *
    *  - ∀f. f[∅] = ∅
    */
  @pure
  @opaque
  def mapEmpty[T, U](f: T => U): Unit = {
    if (Set.empty[T].map(f) =/= Set.empty[U]) {
      val w: U = notEqualsWitness(Set.empty[T].map(f), Set.empty[U])
      emptyContains(w)
      assert(!Set.empty[U].contains(w))
      if (Set.empty[T].map[U](f).contains(w)) {
        val w2 = mapContainsWitness(Set.empty[T], f, w)
        emptyContains(w2)
      }
    }
  }.ensuring(Set.empty[T].map(f) === Set.empty[U])

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------ISEMPTY----------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If two sets are empty then their are equals (i.e. the empty set is unique).
    */
  @pure @opaque
  def isEmptyEquals[T](s1: Set[T], s2: Set[T]): Unit = {
    if (s1.isEmpty && s2.isEmpty) {
      isEmptySubsetOf(s1, s2)
      isEmptySubsetOf(s2, s1)
    }
    if (s1 === s2) {
      unfold(s1.isEmpty)
      unfold(s2.isEmpty)
      equalsTransitivityStrong(Set.empty[T], s1, s2)
    }
  }.ensuring(
    ((s1.isEmpty && s2.isEmpty) ==> (s1 === s2)) &&
      ((s1 === s2) ==> (s1.isEmpty == s2.isEmpty))
  )

  /** If a set is empty, then it a subset of any other set.
    */
  @pure
  @opaque
  def isEmptySubsetOf[T](s1: Set[T], s2: Set[T]): Unit = {
    require(s1.isEmpty)
    unfold(s1.isEmpty)
    emptySubsetOf(s2)
    equalsSubsetOfTransitivity(s1, Set.empty[T], s2)
  }.ensuring(s1.subsetOf(s2))

  /** If a set is empty if and only if it is a subset of the empty set.
    */
  @pure
  @opaque
  def subsetOfEmpty[T](s: Set[T]): Unit = {
    unfold(s.isEmpty)
    emptySubsetOf(s)
  }.ensuring(s.subsetOf(Set.empty) == s.isEmpty)

  /** If a set is empty then it does not contain any element.
    */
  @pure @opaque
  def isEmptyContains[T](s: Set[T], e: T): Unit = {
    require(s.isEmpty)
    unfold(s.isEmpty)
    equalsContains(s, Set.empty[T], e)
    emptyContains(e)
  }.ensuring(!s.contains(e))

  /** If a set is empty for all elements in it any predicate holds.
    */
  @pure
  @opaque
  def forallIsEmpty[T](s: Set[T], p: T => Boolean): Unit = {
    require(s.isEmpty)
    unfold(s.isEmpty)
    forallEquals(s, Set.empty[T], p)
    forallEmpty(p)
  }.ensuring(s.forall(p))

  /** If a set is empty then there exist no element for which a given predicate holds.
    */
  @pure
  @opaque
  def existsIsEmpty[T](s: Set[T], p: T => Boolean): Unit = {
    require(s.isEmpty)
    unfold(s.isEmpty)
    existsEquals(s, Set.empty[T], p)
    emptyExists(p)
  }.ensuring(!s.exists(p))

  /** If a set is not empty, then we can exhibit an element such that is contained in it.
    *
    * @return the element contained in the set
    */
  @pure @opaque
  def notEmptyWitness[T](s: Set[T]): T = {
    require(!s.isEmpty)
    unfold(s.isEmpty)
    val w = notEqualsWitness(s, Set.empty[T])
    emptyContains(w)
    w
  }.ensuring(s.contains)

  /** If a set is empty then it is disjoint to any other set.
    */
  @pure
  @opaque
  def disjointIsEmpty[T](s1: Set[T], s2: Set[T]): Unit = {
    require(s1.isEmpty || s2.isEmpty)
    if (!s1.disjoint(s2)) {
      val w = notDisjointWitness(s1, s2)
      if (s1.isEmpty) {
        isEmptyContains(s1, w)
      } else {
        isEmptyContains(s2, w)
      }
    }
    disjointSymmetry(s1, s2)
  }.ensuring(s1.disjoint(s2) && s2.disjoint(s1))

  /** If a set is empty then it is a neutral element wrt union.
    */
  @pure
  @opaque
  def unionIsEmpty[T](s1: Set[T], s2: Set[T]): Unit = {
    unionEmpty(s1)
    unionEmpty(s2)
    if (s1.isEmpty) {
      unfold(s1.isEmpty)
      unionEqualsLeft(s1, Set.empty[T], s2)
      equalsTransitivity(s1 ++ s2, Set.empty ++ s2, s2)
    }
    if (s2.isEmpty) {
      unfold(s2.isEmpty)
      unionEqualsRight(s1, Set.empty[T], s2)
      equalsTransitivity(s1 ++ s2, s1 ++ Set.empty, s1)
    }
  }.ensuring(
    (s1.isEmpty ==> (s1 ++ s2 === s2)) &&
      (s2.isEmpty ==> (s1 ++ s2 === s1))
  )

  /** If a set is empty and an element is added to it, then the result is equal to the singleton of that element.
    */
  @pure
  @opaque
  def inclIsEmpty[T](s: Set[T], e: T): Unit = {
    require(s.isEmpty)
    unfold(s.isEmpty)
    equalsIncl(s, Set.empty[T], e)
    inclEmpty(e)
    unfold(s.incl(e))
    unfold(Set.empty[T].incl(e))
    equalsTransitivity(s.incl(e), Set.empty[T].incl(e), Set(e))
  }.ensuring(s.incl(e) === Set(e))

  /** If a set is empty then its size is equal to 0.
    */
  @pure
  @opaque
  def isEmptySize[T](s: Set[T]): Unit = {
    if (s.isEmpty) {
      unfold(s.isEmpty)
      sizeEquals(s, Set.empty[T])
      emptySize[T]
    } else {
      val w = notEmptyWitness(s)
      singletonSubsetOf(s, w)
      singletonSize(w)
      subsetOfSize(Set(w), s)
    }
  }.ensuring(s.isEmpty == (s.size == BigInt(0)))

  /** The empty set is empty.
    */
  @pure
  @opaque
  def emptyIsEmpty[T](): Unit = {
    unfold(Set.empty[T].isEmpty)
    equalsReflexivity(Set.empty[T])
  }.ensuring(Set.empty[T].isEmpty)

  /** The image of an empty set is also empty.
    */
  @pure
  @opaque
  def mapIsEmpty[T, U](s: Set[T], f: T => U): Unit = {
    mapEmpty(f)
    unfold(s.map[U](f).isEmpty)
    unfold(s.isEmpty)
    if (s.isEmpty) {
      mapEquals(s, Set.empty[T], f)
      equalsTransitivity(s.map[U](f), Set.empty.map[U](f), Set.empty[U])
    }
    if (s.map[U](f).isEmpty && !s.isEmpty) {
      val w = notEmptyWitness(s)
      mapContains(s, f, w)
      isEmptyContains(s.map[U](f), f(w))
    }
  }.ensuring(s.map[U](f).isEmpty == s.isEmpty)

  /** --------------------------------------------------------------------------------------------------------------- *
    * -----------------------------------------------SINGLETON-------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** A singleton contains an element if and only if the element of the singleton is equal to the latter.
    */
  @pure @opaque
  def singletonContains[T](e: T, o: T): Unit = {
    unfold(Set[T](e).contains)
    val eq: T => Boolean = _ == o
    val neq: T => Boolean = !eq(_)
    forallNotExists(Set(e), eq)
    forallSingleton(e, neq)
  }.ensuring(Set[T](e).contains(o) == (e == o))

  /** A singleton contains its own element.
    */
  @pure @opaque
  def singletonContains[T](e: T): Unit = {
    singletonContains(e, e)
  }.ensuring(Set[T](e).contains(e))

  /** There exists an element satisfying a predicate in a singleton if and only if its element satisfies the predicate.
    */
  @pure
  @opaque
  def singletonExists[T](e: T, f: T => Boolean): Unit = {
    forallNotExists(Set[T](e), f)
    forallSingleton(e, !f(_))
  }.ensuring(Set[T](e).exists(f) == f(e))

  /** A set contains an element if and only if its singleton is subset of the set.
    */
  @pure @opaque
  def singletonSubsetOf[T](s: Set[T], e: T): Unit = {
    unfold(Set[T](e).subsetOf(s))
    forallSingleton(e, s.contains)
  }.ensuring(s.contains(e) == Set[T](e).subsetOf(s))

  /** Two elements are the same if and only if the singleton of the first is subset of the singleton of the other.
    */
  @pure
  @opaque
  def twoSingletonSubsetOf[T](a: T, b: T): Unit = {
    singletonSubsetOf(Set(b), a)
    singletonContains(b, a)
  }.ensuring((a == b) == Set[T](a).subsetOf(Set[T](b)))

  /** Two elements are the same if and only if the singleton of the first is equal of the singleton of the other.
    */
  @pure
  @opaque
  def twoSingletonEquals[T](a: T, b: T): Unit = {
    twoSingletonSubsetOf(a, b)
    twoSingletonSubsetOf(b, a)
  }.ensuring((a == b) == (Set[T](a) === Set[T](b)))

  /** If a set is a subset of a singleton then either they are equal or the set is empty.
    */
  @pure
  @opaque
  def subsetOfSingleton[T](s: Set[T], e: T): Unit = {

    // ==>
    if (s.isEmpty) {
      isEmptySubsetOf(s, Set[T](e))
      assert(s.subsetOf(Set[T](e)))
    }

    // <==
    if (s.subsetOf(Set[T](e))) {
      subsetOfSize(s, Set[T](e))
      singletonSize(e)
      sizePositive(s)

      isEmptySize[T](s)

      if (s.size == BigInt(1)) {
        val w = sizeOneWitness(s)
        equalsSubsetOfTransitivity(Set[T](w), s, Set[T](e))
        twoSingletonSubsetOf(w, e)
      }
    }
  }.ensuring((s.isEmpty || s === Set[T](e)) == s.subsetOf(Set[T](e)))

  /** Being disjoint from a singleton is equivalent to not containing its element
    */
  @pure @opaque
  def disjointSingleton[T](s: Set[T], e: T): Unit = {
    if (s.disjoint(Set(e))) {
      disjointContains(s, Set(e), e)
      singletonContains(e)
    } else {
      val w = notDisjointWitness(s, Set(e))
      singletonContains(e, w)
    }
    disjointSymmetry(s, Set(e))
  }.ensuring(
    (s.disjoint(Set(e)) == !s.contains(e)) &&
      (Set(e).disjoint(s) == !s.contains(e))
  )

  /** If two singleton are disjoint then their element are different
    */
  @pure
  @opaque
  def disjointTwoSingleton[T](a: T, b: T): Unit = {
    singletonContains(a, b)
    disjointSingleton(Set(a), b)
  }.ensuring(Set(a).disjoint(Set(b)) == (a != b))

  /** A set contains an element if and only if the intersection with its singleton is the singleton itself.
    */
  @pure
  @opaque
  def intersectSingleton[T](s: Set[T], e: T): Unit = {
    intersectAltDefinition(Set(e), s)
    singletonSubsetOf(s, e)
  }.ensuring(
    (s.intersect(Set(e)) === Set(e)) == s.contains(e)
  )

  /** If the size of a set is positive then it has an element and we can exhibit it.
    */
  @pure
  @opaque
  def sizePositiveWitness[T](s: Set[T]): T = {
    require(s.size >= BigInt(1))
    isEmptySize(s)
    notEmptyWitness(s)
  }.ensuring(res => s.contains(res))

  /** If the size of a set is equal to one then it is a singleton. Furthermore we can exhibit its unique element.
    */
  @pure
  @opaque
  def sizeOneWitness[T](s: Set[T]): T = {
    require(s.size == BigInt(1))
    val res = sizePositiveWitness(s)
    singletonSubsetOf(s, res)
    if (!s.subsetOf(Set(res))) {
      val w = notSubsetOfWitness(s, Set(res))

      // (Set(res) ++ Set(w)).size == 2
      singletonContains(res, w)
      disjointTwoSingleton(res, w)
      unionDisjointSize(Set(res), Set(w))
      singletonSize(res)
      singletonSize(w)

      // s.size >= 2
      unionSubsetOf(Set(res), Set(w), s)
      singletonSubsetOf(s, res)
      singletonSubsetOf(s, w)
      subsetOfSize(Set(res) ++ Set(w), s)
    }

    res
  }.ensuring(res => s === Set[T](res))

  /** If the union of two sets is equal to a singleton then either:
    *  - Both sets are equal to this singleton
    *  - A set is equal to this singleton and the other one is empty
    */
  @pure @opaque
  def unionEqualsSingleton[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    require((s1 ++ s2) === Set(e))
    unionSubsetOf(s1, s2, Set(e))
    subsetOfSingleton(s1, e)
    subsetOfSingleton(s2, e)
    unionIsEmpty(s1, s2)
    if (s1.isEmpty) {
      equalsTransitivity(Set(e), s1 ++ s2, s2)
    }
    if (s2.isEmpty) {
      equalsTransitivity(Set(e), s1 ++ s2, s1)
    }
  }.ensuring(
    (s1.isEmpty && s2 === Set(e)) ||
      (s1 === Set(e) && s2.isEmpty) ||
      (s1 === Set(e) && s2 === Set(e))
  )

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------SUBSET OF--------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If a set is not a subset of an other one then we can exhibit an element that is in the former but not in the
    * latter.
    */
  @pure @opaque
  def notSubsetOfWitness[T](s1: Set[T], s2: Set[T]): T = {
    require(!s1.subsetOf(s2))
    unfold(s1.subsetOf(s2))
    val d = notForallWitness(s1, s2.contains)
    d
  }.ensuring(res => s1.contains(res) && !s2.contains(res))

  /** Pingeonhole principle
    *
    * If a set is bigger then an other one we can find an element that is in the former but not in the latter.
    */
  @pure
  @opaque
  def pigeonhole[T](s1: Set[T], s2: Set[T]): T = {
    require(s1.size > s2.size)
    intersectSize(s1, s2)
    diffSize(s1, s2)
    isEmptySize(s1 &~ s2)
    val res = notEmptyWitness(s1 &~ s2)
    diffContains(s1, s2, res)
    res
  }.ensuring(res => s1.contains(res) && !s2.contains(res))

  /** If two sets are not equal then we can find an element that is in one of the two sets but not the other one.
    */
  @pure
  @opaque
  def notEqualsWitness[T](s1: Set[T], s2: Set[T]): T = {
    require(s1 =/= s2)
    val d =
      if (!s1.subsetOf(s2))
        notSubsetOfWitness(s1, s2)
      else
        notSubsetOfWitness(s2, s1)
    d
  }.ensuring(res => s1(res) != s2(res))

  /** If a set contains an element but a second one does not contain it then the former cannot be a subset of the
    * latter.
    */
  @pure @opaque
  def isDifferentiatedByNotSubset[T](s1: Set[T], s2: Set[T], d: T): Unit = {
    require(s1.contains(d))
    require(!s2.contains(d))

    if (s1.subsetOf(s2)) {
      subsetOfContains(s1, s2, d)
      Unreachable()
    }
  }.ensuring(
    !s1.subsetOf(s2)
  )

  /** All the elements of a subset belong to the superset.
    *
    *  - ∀s1,s2. s1 ⊆ s2 => (∀x. x ∈ s1 => x ∈ s2)
    */
  @pure
  @opaque
  def subsetOfContains[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    require(s1.subsetOf(s2))
    unfold(s1.subsetOf(s2))
    if (s1(e)) {
      forallContains(s1, s2.contains, e)
    }
  }.ensuring(
    (s1(e) ==> s2(e)) &&
      (!s2(e) ==> !s1(e))
  )

  /** The size of a subset is smaller then the size of its superset.
    *
    *  - ∀s1,s2. s1 ⊆ s2 => |s1| ≤ |s2|
    */
  @pure @opaque
  def subsetOfSize[T](s1: Set[T], s2: Set[T]): Unit = {
    require(s1.subsetOf(s2))
    unionAltDefinition(s1, s2)
    unionSize(s1, s2)
    sizeEquals(s1 ++ s2, s2)
  }.ensuring(s1.size <= s2.size)

  /** Every set is subset to itself (i.e. subsetOf is a reflexive relation).
    *
    *  - ∀s. s ⊆ s
    */
  @pure @opaque
  def subsetOfReflexivity[T](s: Set[T]): Unit = {
    if (!s.subsetOf(s)) {
      val d = notSubsetOfWitness(s, s)
      Unreachable()
    }
  }.ensuring(s.subsetOf(s))

  /** SubsetOf is a transitive relation.
    *
    *  - ∀s1,s2,s3. s1 ⊆ s2 /\ s2 ⊆ s3 => s1 ⊆ s3
    */
  @pure @opaque
  def subsetOfTransitivity[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    require(s1.subsetOf(s2))
    require(s2.subsetOf(s3))
    if (!s1.subsetOf(s3)) {
      val d = notSubsetOfWitness(s1, s3)
      subsetOfContains(s1, s2, d)
      subsetOfContains(s2, s3, d)
      Unreachable()
    }
  }.ensuring(s1.subsetOf(s3))

  /** If a set is equal to another then the former is a subset of a third set if and only if the latter also is.
    *
    *  - ∀s1,s2,s3. s1 = s2 => (s1 ⊆ s3 <=> s2 ⊆ s3)
    */
  @pure
  @opaque
  def equalsSubsetOfTransitivity[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    require(s1 === s2)

    if (s1.subsetOf(s3)) {
      subsetOfTransitivity(s2, s1, s3)
    }
    if (s2.subsetOf(s3)) {
      subsetOfTransitivity(s1, s2, s3)
    }

  }.ensuring(
    s1.subsetOf(s3) == s2.subsetOf(s3)
  )

  /** If a set is equal to another then the former is a superset of a third set if and only if the latter also is.
    *
    *  - ∀s1,s2,s3. s2 = s3 => (s1 ⊆ s2 <=> s1 ⊆ s3)
    */
  @pure
  @opaque
  def subsetOfEqualsTransitivity[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    require(s2 === s3)

    if (s1.subsetOf(s2)) {
      subsetOfTransitivity(s1, s2, s3)
    }
    if (s1.subsetOf(s3)) {
      subsetOfTransitivity(s1, s3, s2)
    }

  }.ensuring(
    s1.subsetOf(s2) == s1.subsetOf(s3)
  )

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------EQUALS------------------------------------------------------ *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If two equals are equals then an element belongs to the former if and only if it belongs to the latter (ie. set
    * equality is congruent wrt contains).
    *
    *  - ∀s1,s2. s1 = s2 => (∀x. x ∈ s1 <=> x ∈ s2)
    */
  @pure
  @opaque
  def equalsContains[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    require(s1 === s2)
    subsetOfContains(s1, s2, e)
    subsetOfContains(s2, s1, e)
  }.ensuring(
    s1(e) == s2(e)
  )

  /** Set equality is reflexive
    *
    *  - ∀s. s = s
    */
  @pure
  @opaque
  def equalsReflexivity[T](s: Set[T]): Unit = {
    subsetOfReflexivity(s)
  }.ensuring(
    s === s
  )

  /** Set equality is transitive
    *
    *  - ∀s1,s2,s3. s2 = s3 => (s1 = s2 <=> s1 = s3)
    */
  @pure
  @opaque
  def equalsTransitivityStrong[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    require(s2 === s3)
    subsetOfEqualsTransitivity(s1, s2, s3)
    equalsSubsetOfTransitivity(s2, s3, s1)
  }.ensuring((s1 === s2) == (s1 === s3))

  /** Set equality is transitive
    *
    *  - ∀s1,s2,s3. s1 = s2 /\ s2 = s3 => s1 = s3
    */
  @pure
  @opaque
  def equalsTransitivity[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    require(s1 === s2)
    require(s2 === s3)
    equalsTransitivityStrong(s1, s2, s3)
  }.ensuring(s1 === s3)

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------UNION------------------------------------------------------ *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If an union between two sets contains an element then at least one of the two sets contain the element.
    *
    *  - ∀s1,s2,x. x ∈ s1 ∪ s2 => (x ∈ s1 \/ x ∈ s2)
    */
  @pure
  @opaque
  def unionContains[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    unfold((s1 ++ s2).contains)
    unfold(s1.contains)
    unfold(s2.contains)
    val eq: T => Boolean = _ == e
    val neq: T => Boolean = !eq(_)
    forallNotExists(s1 ++ s2, eq)
    forallNotExists(s1, eq)
    forallNotExists(s2, eq)
    forallUnion(s1, s2, neq)
  }.ensuring((s1 ++ s2)(e) == (s1(e) || s2(e)))

  /** Alternative definition of the subset relation.
    *
    * A set is subset of another one if and only if the union of both is equal to the latter.
    */
  @pure
  @opaque
  def unionAltDefinition[T](s1: Set[T], s2: Set[T]): Unit = {
    unionSubsetOf(s1, s2, s2)
    subsetOfReflexivity(s2)
    subsetOfUnion(s1, s2)
    unionCommutativity(s1, s2)
    equalsTransitivityStrong(s2, s2 ++ s1, s1 ++ s2)
  }.ensuring(
    (s1.subsetOf(s2) == (s1 ++ s2 === s2)) &&
      (s1.subsetOf(s2) == (s2 ++ s1 === s2))
  )

  /** The union between a set and itself gives the original set (i.e. the union is an idempotent operator).
    *
    *  - ∀s. s ∪ s = s
    */
  @pure
  @opaque
  def unionIdempotence[T](s: Set[T]): Unit = {
    subsetOfReflexivity(s)
    unionAltDefinition(s, s)
  }.ensuring(s ++ s === s)

  /** If there exists in the union of two sets, an element satisfying a given predicate then the predicate is either
    * satisfied by an element in the first set or the second one.
    *
    *  - ∀s1,s2,f. (∃x. x ∈ s1 ∪ s2 /\ f(x)) <=> ((∃x. x ∈ s1 /\ f(x)) \/ (∃x. x ∈ s2 /\ f(x)))
    */
  @pure
  @opaque
  def unionExists[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {
    SetAxioms.forallNotExists(s1, f)
    SetAxioms.forallNotExists(s2, f)
    SetAxioms.forallNotExists(s1 ++ s2, f)
    SetAxioms.forallUnion(s1, s2, !f(_))
  }.ensuring((s1 ++ s2).exists(f) == (s1.exists(f) || s2.exists(f)))

  /** The union between two sets is subset of a third set if and only if both are subset of the latter.
    *
    *  - ∀s1,s2,s3. s1 ∪ s2 ⊆ s3 <=> (s1 ⊆ s3 /\ s2 ⊆ s3)
    */
  @pure
  @opaque
  def unionSubsetOf[T](s1: Set[T], s2: Set[T], s: Set[T]): Unit = {
    unfold((s1 ++ s2).subsetOf(s))
    unfold(s1.subsetOf(s))
    unfold(s2.subsetOf(s))
    forallUnion(s1, s2, s.contains)
  }.ensuring((s1 ++ s2).subsetOf(s) == (s1.subsetOf(s) && s2.subsetOf(s)))

  /** Any set is subset of its union with an other set.
    *
    *  - ∀s1,s2. s1 ⊆ s1 ∪ s2 /\ s2 ⊆ s1 ∪ s2
    */
  @pure @opaque
  def subsetOfUnion[T](s1: Set[T], s2: Set[T]): Unit = {
    if (!s1.subsetOf(s1 ++ s2)) {
      val d = notSubsetOfWitness(s1, s1 ++ s2)
      unionContains(s1, s2, d)
    }
    if (!s2.subsetOf(s1 ++ s2)) {
      val d = notSubsetOfWitness(s2, s1 ++ s2)
      unionContains(s1, s2, d)
    }
  }.ensuring(
    s1.subsetOf(s1 ++ s2) &&
      s2.subsetOf(s1 ++ s2)
  )

  /** If a set is subset of another one it is also a subset of the union between latter and any third set.
    *
    *  - ∀s,s1,s2. s ⊆ s1 \/ s ⊆ s2 => s ⊆ s1 ∪ s2
    */
  @pure
  @opaque
  def subsetOfUnion[T](s: Set[T], s1: Set[T], s2: Set[T]): Unit = {
    require(s.subsetOf(s1) || s.subsetOf(s2))
    subsetOfUnion(s1, s2)
    if (s.subsetOf(s1)) {
      subsetOfTransitivity(s, s1, s1 ++ s2)
    } else {
      subsetOfTransitivity(s, s2, s1 ++ s2)
    }
  }.ensuring(
    s.subsetOf(s1 ++ s2)
  )

  /** If a set is subset of another then its union with a third set is also subset of the union of the second set.
    */
  @pure
  @opaque
  def unionSubsetOfRight[T](s: Set[T], s1: Set[T], s2: Set[T]): Unit = {
    require(s1.subsetOf(s2))
    subsetOfReflexivity(s)
    unionSubsetOf(s, s1, s ++ s2)
    subsetOfUnion(s, s, s2)
    subsetOfUnion(s1, s, s2)
  }.ensuring((s ++ s1).subsetOf(s ++ s2))

  /** If two sets are equal then their union with a third set is equal as well.
    */
  @pure
  @opaque
  def unionEqualsRight[T](s: Set[T], s1: Set[T], s2: Set[T]): Unit = {
    require(s1 === s2)
    equalsReflexivity(s)
    unionEquals(s, s, s1, s2)
  }.ensuring((s ++ s1) === (s ++ s2))

  /** If a set is subset of another then its union with a third set is also subset of the union of the second set.
    */
  @pure
  @opaque
  def unionSubsetOfLeft[T](s1: Set[T], s2: Set[T], s: Set[T]): Unit = {
    require(s1.subsetOf(s2))
    subsetOfReflexivity(s)
    unionSubsetOf(s1, s, s2 ++ s)
    subsetOfUnion(s, s2, s)
    subsetOfUnion(s1, s2, s)
  }.ensuring((s1 ++ s).subsetOf(s2 ++ s))

  /** If two sets are equal then their union with a third set is equal as well.
    */
  @pure
  @opaque
  def unionEqualsLeft[T](s1: Set[T], s2: Set[T], s: Set[T]): Unit = {
    require(s1 === s2)
    equalsReflexivity(s)
    unionEquals(s1, s2, s, s)
  }.ensuring((s1 ++ s) === (s2 ++ s))

  /** If two pairs of set are subset one of the other, then their unions are also subset of the other.
    */
  @pure
  @opaque
  def unionSubsetOf[T](s11: Set[T], s12: Set[T], s21: Set[T], s22: Set[T]): Unit = {
    require(s11.subsetOf(s12))
    require(s21.subsetOf(s22))
    unionSubsetOfLeft(s11, s12, s21)
    unionSubsetOfRight(s12, s21, s22)
    subsetOfTransitivity(s11 ++ s21, s12 ++ s21, s12 ++ s22)
  }.ensuring((s11 ++ s21).subsetOf(s12 ++ s22))

  /** If two pairs of set are equal, then their unions also are.
    */
  @pure
  @opaque
  def unionEquals[T](s11: Set[T], s12: Set[T], s21: Set[T], s22: Set[T]): Unit = {
    require(s11 === s12)
    require(s21 === s22)
    unionSubsetOf(s11, s12, s21, s22)
    unionSubsetOf(s12, s11, s22, s21)
  }.ensuring(s11 ++ s21 === s12 ++ s22)

  /** Union is a commutative operation.
    */
  @pure
  @opaque
  def unionCommutativity[T](s1: Set[T], s2: Set[T]): Unit = {
    unionSubsetOf(s1, s2, s2 ++ s1)
    subsetOfUnion(s2, s1)
    unionSubsetOf(s2, s1, s1 ++ s2)
    subsetOfUnion(s1, s2)
  }.ensuring(s1 ++ s2 === s2 ++ s1)

  /** Union is an associative operation.
    */
  @pure
  @opaque
  def unionAssociativity[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    // (s1 ++ s2) ++ s3 < s1 ++ (s2 ++ s3)
    unionSubsetOf(s1 ++ s2, s3, s1 ++ (s2 ++ s3))
    unionSubsetOf(s1, s2, s1 ++ (s2 ++ s3))
    subsetOfUnion(s2, s3)
    subsetOfUnion(s1, s2 ++ s3)
    subsetOfTransitivity(s2, s2 ++ s3, s1 ++ (s2 ++ s3))
    subsetOfTransitivity(s3, s2 ++ s3, s1 ++ (s2 ++ s3))

    // (s1 ++ s2) ++ s3 > s1 ++ (s2 ++ s3)
    unionSubsetOf(s1, s2 ++ s3, (s1 ++ s2) ++ s3)
    unionSubsetOf(s2, s3, (s1 ++ s2) ++ s3)
    subsetOfUnion(s1, s2)
    subsetOfUnion(s1 ++ s2, s3)
    subsetOfTransitivity(s1, s1 ++ s2, (s1 ++ s2) ++ s3)
    subsetOfTransitivity(s2, s1 ++ s2, (s1 ++ s2) ++ s3)

  }.ensuring((s1 ++ s2) ++ s3 === s1 ++ (s2 ++ s3))

  /** The union between two sets is emtpy if and only if both are empty.
    */
  @pure
  @opaque
  def isEmptyUnion[T](s1: Set[T], s2: Set[T]): Unit = {
    subsetOfEmpty(s1 ++ s2)
    subsetOfEmpty(s1)
    subsetOfEmpty(s2)
    unionSubsetOf(s1, s2, Set.empty[T])
  }.ensuring((s1 ++ s2).isEmpty == (s1.isEmpty && s2.isEmpty))

  /** The union of two sets is equal to the union between the first one and the difference between the second and the
    * first one.
    */
  @pure
  @opaque
  def unionDiffDef[T](s1: Set[T], s2: Set[T]): Unit = {
    if ((s1 ++ s2) =/= (s1.diff(s2) ++ s2)) {
      val w = notEqualsWitness(s1 ++ s2, s1.diff(s2) ++ s2)
      unionContains(s1, s2, w)
      unionContains(s1.diff(s2), s2, w)
      diffContains(s1, s2, w)
    }
    if ((s1 ++ s2) =/= (s1 ++ s2.diff(s1))) {
      val w = notEqualsWitness(s1 ++ s2, s1 ++ s2.diff(s1))
      unionContains(s1, s2, w)
      unionContains(s1, s2.diff(s1), w)
      diffContains(s2, s1, w)
    }
  }.ensuring(
    ((s1 ++ s2) === (s1.diff(s2) ++ s2)) &&
      ((s1 ++ s2) === (s1 ++ s2.diff(s1)))
  )

  /** Inclusion-exclusion principle
    *
    * The size of the union between two sets is greater or equal than the size of both sets but smaller or equal than
    * the sum of the sizes. It is equal to the sum of the size of both sets minus the size of the intersection.
    */
  @pure @opaque
  def unionSize[T](s1: Set[T], s2: Set[T]): Unit = {

    // (s1 ++ s2).size >= s2.size
    unionDiffDef(s1, s2)
    sizeEquals(s1 ++ s2, s1.diff(s2) ++ s2)
    diffDisjoint(s1, s2)
    unionDisjointSize(s1.diff(s2), s2)
    sizePositive(s1.diff(s2))

    // (s1 ++ s2).size >= s1.size
    sizeEquals(s1 ++ s2, s1 ++ s2.diff(s1))
    diffDisjoint(s2, s1)
    disjointSymmetry(s2.diff(s1), s1)
    unionDisjointSize(s1, s2.diff(s1))
    sizePositive(s2.diff(s1))

    // (s1 ++ s2).size <= s1.size + s2.size
    diffSize(s1, s2)
    sizePositive(s1 & s2)

  }.ensuring(
    (s1 ++ s2).size <= s1.size + s2.size &&
      ((s1 ++ s2).size == s1.size + s2.size - (s1 & s2).size) &&
      ((s1 ++ s2).size >= s2.size) &&
      ((s1 ++ s2).size >= s1.size)
  )

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------INCL-------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If a set is subset of an other one then adding a value to the second set or on both sides does not change the
    * subset relationship. Furthermore, if one the two sets already contains the value, adding it to the first set
    * also does not change the relationship.
    */
  @pure
  @opaque
  def subsetOfIncl[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    require(s1.subsetOf(s2))
    subsetOfTransitivity(s1, s2, s2 + e)
    unfold(s1.incl(e))
    unfold(s2.incl(e))
    unionSubsetOfLeft(s1, s2, Set(e))
    unionSubsetOf(s1, Set(e), s2)
    if (s1(e)) {
      subsetOfContains(s1, s2, e)
    }
    if (s1(e) || s2(e)) {
      singletonSubsetOf(s2, e)
    }

  }.ensuring(
    s1.subsetOf(s2 + e) &&
      (s1 + e).subsetOf(s2 + e) &&
      ((s1(e) || s2(e)) ==> (s1 + e).subsetOf(s2))
  )

  /** If two sets are equal then they stay equal when adding a value on both sides.
    * Futhermore if one of the sets contains the value, this also holds when adding it on
    * one side only.
    */
  @pure
  @opaque
  def equalsIncl[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    require(s1 === s2)
    subsetOfIncl(s1, s2, e)
    subsetOfIncl(s2, s1, e)
  }.ensuring(
    s1 + e === s2 + e &&
      ((s1(e) || s2(e)) ==> (s1 + e === s2 && s1 === s2 + e))
  )

  /** A predicate holds for all elements of a set for which a value has been added to it if and only if it holds for all
    * elements of the set and for the value.
    */
  @pure
  @opaque
  def forallIncl[T](s: Set[T], e: T, p: T => Boolean): Unit = {
    unfold(s.incl(e))
    forallSingleton(e, p)
    forallUnion(s, Set[T](e), p)
  }.ensuring((s + e).forall(p) == (s.forall(p) && p(e)))

  /** There exists an element satisfying a predicate in a set with a value added to it if an only if the predicate is
    * satisfied for an element in the original set or if is true for the value.
    */
  @pure
  @opaque
  def inclExists[T](s: Set[T], e: T, f: T => Boolean): Unit = {
    SetAxioms.forallNotExists(s, f)
    SetAxioms.forallNotExists(s + e, f)
    forallIncl(s, e, !f(_))
  }.ensuring((s + e).exists(f) == (s.exists(f) || f(e)))

  /** A set with a value added to it contains an element if an only if the set contains this element or if it is equal
    * to the value.
    */
  @pure @opaque
  def inclContains[T](s: Set[T], add: T, e: T): Unit = {
    unfold(s.incl(add))
    unionContains(s, Set(add), e)
    singletonContains(add, e)
  }.ensuring((s + add).contains(e) == (s.contains(e) || add == e))

  /** Making the union of two sets and then adding an element is equivalent to adding an element to the second set and
    * then perform the union.
    */
  @pure
  @opaque
  def unionInclAssociativity[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    unfold((s1 ++ s2).incl(e))
    unfold(s2.incl(e))
    unionAssociativity(s1, s2, Set(e))
  }.ensuring((s1 ++ s2) + e === s1 ++ (s2 + e))

  /** When adding two elements to a set, the order of the addition does not matter.
    */
  @pure
  @opaque
  def inclCommutativity[T](s: Set[T], e1: T, e2: T): Unit = {

    unfold(Set(e1).incl(e2))
    unfold(Set(e2).incl(e1))
    unfold(s.incl(e1))
    unfold(s.incl(e2))
    unionCommutativity(Set(e1), Set(e2))
    unionEqualsRight(s, Set(e1) + e2, Set(e2) + e1)
    unionInclAssociativity(s, Set(e1), e2)
    unionInclAssociativity(s, Set(e2), e1)
    equalsTransitivity((s ++ Set(e1)) + e2, s ++ (Set(e1) + e2), s ++ (Set(e2) + e1))
    equalsTransitivity((s ++ Set(e1)) + e2, s ++ (Set(e2) + e1), (s ++ Set(e2)) + e1)
  }.ensuring((s + e1) + e2 === (s + e2) + e1)

  /** --------------------------------------------------------------------------------------------------------------- *
    * ---------------------------------------------FORALL/EXISTS------------------------------------------------------ *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If predicate is not satisfied for all the elements of a set, we can exhibit an element in the set such that the
    * predicate is not satisfied.
    */
  @pure
  @opaque
  @inlineOnce
  def notForallWitness[T](s: Set[T], f: T => Boolean): T = {
    require(!s.forall(f))
    notForallExists(s, f)
    s.witness(!f(_))
  }.ensuring(w => s.contains(w) && !f(w))

  /** If a predicate is valid for all elements of a set and the same set contains an element, then this predicate is
    * valid for the element.
    */
  @pure
  @opaque
  def forallContains[T](s: Set[T], f: T => Boolean, e: T): Unit = {
    require(s.forall(f))
    require(s.contains(e))
    val nf: T => Boolean = !f(_)
    if (nf(e)) {
      witnessExists(s, nf, e)
      notForallExists(s, f)
    }
  }.ensuring(f(e))

  /** If a set is subset of an other and a predicate is valid for all elements of the latter, then it also is for all
    * elements of the former
    */
  @pure @opaque
  def forallSubsetOf[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {
    require(s1.subsetOf(s2))
    require(s2.forall(f))
    if (!s1.forall(f)) {
      val nf: T => Boolean = !f(_)
      val w = notForallWitness(s1, f)
      subsetOfContains(s1, s2, w)
      forallContains(s2, f, w)
      Unreachable()
    }
  }.ensuring(s1.forall(f))

  /** If a set is subset of an other and a predicate is valid for one of the elements of the former, then it also is
    * for an element of the latter.
    */
  @pure
  @opaque
  def existsSubsetOf[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {
    require(s1.subsetOf(s2))
    require(s1.exists(f))
    forallNotExists(s1, f)
    forallNotExists(s2, f)
    if (!s2.exists(f)) {
      forallSubsetOf(s1, s2, x => !f(x))
    }
  }.ensuring(s2.exists(f))

  /** If two sets are equal then a predicate is true for all the elements of the first set if and only if it is true for
    * all the elements of the second one.
    */
  @pure
  @opaque
  def forallEquals[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {
    require(s1 === s2)
    if (s2.forall(f)) {
      forallSubsetOf(s1, s2, f)
    }
    if (s1.forall(f)) {
      forallSubsetOf(s2, s1, f)
    }

  }.ensuring(s1.forall(f) == s2.forall(f))

  /** If two sets are equal there exist an element satisfying a given predicate in the first one if and only if it is
    * also the case in the second one.
    */
  @pure
  @opaque
  def existsEquals[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {
    require(s1 === s2)
    if (s2.exists(f)) {
      existsSubsetOf(s2, s1, f)
    }
    if (s1.exists(f)) {
      existsSubsetOf(s1, s2, f)
    }

  }.ensuring(s1.exists(f) == s2.exists(f))

  /** Double negation rule inside a forall predicate.
    */
  @pure
  @opaque
  def notNotForall[T](s: Set[T], f: T => Boolean): Unit = {
    val nnf: T => Boolean = x => !(!f(x))
    if (!s.forall(f) && s.forall(nnf)) {
      val w = notForallWitness(s, f)
      forallContains(s, nnf, w)
    }
    if (!s.forall(nnf) && s.forall(f)) {
      val w = notForallWitness(s, nnf)
      forallContains(s, f, w)
    }
  }.ensuring(s.forall(f) == s.forall(x => !(!f(x))))

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------FILTER------------------------------------------------------ *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** A set filtered by a predicate contains an element if and only if the element is both in the set and satisfies
    * the predicate.
    */
  @pure @opaque
  def filterContains[T](s: Set[T], f: T => Boolean, e: T): Unit = {

    unfold(s.contains)
    unfold(s.filter(f).contains)
    forallNotExists(s, _ == e)
    forallNotExists(s.filter(f), _ == e)
    forallFilter(s, f, x => !(x == e))

    val g: T => Boolean = x => f(x) ==> !(x == e)

    if (!s.filter(f).contains(e)) {
      if (s.contains(e)) {
        forallContains(s, g, e)
      }
    } else {
      val w = notForallWitness(s, g)
    }
  }.ensuring(s.filter(f).contains(e) == (f(e) && s.contains(e)))

  /** A set filtered by a predicate is empty if and only if all the elements do not satisfy the predicate.
    */
  @pure @opaque @inlineOnce
  def filterIsEmpty[T](s: Set[T], f: T => Boolean): Unit = {
    if (!s.forall(!f(_)) && s.filter(f).isEmpty) {
      val w = notForallWitness(s, !f(_))
      filterContains(s, f, w)
      isEmptyContains(s.filter(f), w)
    }
    if (!s.filter(f).isEmpty && s.forall(!f(_))) {
      val w = notEmptyWitness(s.filter(f))
      filterContains(s, f, w)
      forallContains(s, !f(_), w)
    }
  }.ensuring(s.filter(f).isEmpty == s.forall(!f(_)))

  /** If a set is subset of an other then they stay subset when they are filtered by some predicate.
    */
  @pure @opaque
  def filterSubsetOf[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {
    require(s1.subsetOf(s2))
    if (!s1.filter(f).subsetOf(s2.filter(f))) {
      val w = notSubsetOfWitness(s1.filter(f), s2.filter(f))
      filterContains(s1, f, w)
      filterContains(s2, f, w)
      subsetOfContains(s1, s2, w)
    }
  }.ensuring(s1.filter(f).subsetOf(s2.filter(f)))

  /** If two sets are equal then they stay equal when they are filtered by some predicate.
    */
  @pure
  @opaque
  def filterEquals[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {
    require(s1 === s2)
    filterSubsetOf(s1, s2, f)
    filterSubsetOf(s2, s1, f)
  }.ensuring(s1.filter(f) === s2.filter(f))

  /** A set filter by a predicate is always a subset of the original set.
    */
  @pure @opaque
  def filterSubsetOf[T](s: Set[T], f: T => Boolean): Unit = {
    if (!s.filter(f).subsetOf(s)) {
      val w = notSubsetOfWitness(s.filter(f), s)
      filterContains(s, f, w)
    }
  }.ensuring(s.filter(f).subsetOf(s))

  /** A set is equal to its filter if and only if all its elements satisfy the predicate.
    */
  @pure
  @opaque
  def filterEquals[T](s: Set[T], f: T => Boolean): Unit = {
    filterSubsetOf(s, f)
    if (s.forall(f) && !s.subsetOf(s.filter(f))) {
      val w = notSubsetOfWitness(s, s.filter(f))
      filterContains(s, f, w)
      forallContains(s, f, w)
    }
    if (s.subsetOf(s.filter(f)) && !s.forall(f)) {
      val w = notForallWitness(s, f)
      subsetOfContains(s, s.filter(f), w)
      filterContains(s, f, w)
    }
  }.ensuring((s === s.filter(f)) == s.forall(f))

  /** --------------------------------------------------------------------------------------------------------------- *
    * ---------------------------------------------DIFF/REMOVE-------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If the difference between two sets contains an element then the first set contains it but not the second one.
    */
  @pure @opaque
  def diffContains[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    unfold(s1.diff(s2))
    filterContains(s1, !s2.contains(_), e)
  }.ensuring((s1 &~ s2).contains(e) == (s1.contains(e) && !s2.contains(e)))

  /** If a set is subset of an other then their differences with a third set is preserves the subset relationship.
    * If the subset relation happens on the other side, then the relationship is inverted.
    */
  @pure @opaque
  def diffSubsetOf[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    unfold(s1.diff(s3))
    unfold(s2.diff(s3))
    if (s1.subsetOf(s2)) {
      filterSubsetOf(s1, s2, x => !s3.contains(x))
    }
    if (s2.subsetOf(s3) && !(s1 &~ s3).subsetOf(s1 &~ s2)) {
      val w = notSubsetOfWitness(s1 &~ s3, s1 &~ s2)
      diffContains(s1, s3, w)
      diffContains(s1, s2, w)
      subsetOfContains(s2, s3, w)
    }
  }.ensuring(
    (s1.subsetOf(s2) ==> (s1 &~ s3).subsetOf(s2 &~ s3)) &&
      (s2.subsetOf(s3) ==> (s1 &~ s3).subsetOf(s1 &~ s2))
  )

  /** If two pairs of set are subset of an other then their differences preserves the subset relationship on the left
    * and inverts it on the right.
    */
  @pure
  @opaque
  def diffSubsetOf[T](s1: Set[T], s2: Set[T], s3: Set[T], s4: Set[T]): Unit = {
    require(s1.subsetOf(s2))
    require(s4.subsetOf(s3))
    diffSubsetOf(s1, s2, s3)
    diffSubsetOf(s2, s4, s3)
    subsetOfTransitivity(s1 &~ s3, s2 &~ s3, s2 &~ s4)
  }.ensuring(
    (s1 &~ s3).subsetOf(s2 &~ s4)
  )

  /** If two sets are equal then their differences with a third set are also equal.
    */
  @pure
  @opaque
  def diffEquals[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    diffSubsetOf(s1, s2, s3)
    diffSubsetOf(s2, s1, s3)
    diffSubsetOf(s1, s3, s2)
  }.ensuring(
    ((s1 === s2) ==> ((s1 &~ s3) === (s2 &~ s3))) &&
      ((s2 === s3) ==> ((s1 &~ s3) === (s1 &~ s2)))
  )

  /** If two pairs of sets are equal then their differences are equal as well.
    */
  @pure
  @opaque
  def diffEquals[T](s1: Set[T], s2: Set[T], s3: Set[T], s4: Set[T]): Unit = {
    require(s1 === s2)
    require(s3 === s4)
    diffSubsetOf(s1, s2, s3, s4)
    diffSubsetOf(s2, s1, s4, s3)
  }.ensuring(
    (s1 &~ s3) === (s2 &~ s4)
  )

  /** If a set is empty, then its difference with any other set is also empty. Furthermore, the difference of a set and
    * a second one is empty if and only if the former is a subset of the latter.
    */
  @pure
  @opaque
  def diffIsEmpty[T](s1: Set[T], s2: Set[T]): Unit = {
    unfold(s1.diff(s2))
    unfold(s1.subsetOf(s2))
    filterIsEmpty(s1, !s2.contains(_))
    notNotForall(s1, s2.contains)
    if (s1.isEmpty) {
      isEmptySubsetOf(s1, s2)
    }
  }.ensuring(
    ((s1 &~ s2).isEmpty == s1.subsetOf(s2)) &&
      (s1.isEmpty ==> (s1 &~ s2).isEmpty)
  )

  /** A set difference is always disjoint with the second set. Furthermore the difference between a first set
    * and a second one is disjoint from the difference of the second and the first one.
    */
  @pure
  @opaque
  def diffDisjoint[T](s1: Set[T], s2: Set[T]): Unit = {
    if (!(s1 &~ s2).disjoint(s2)) {
      val w = notDisjointWitness(s1 &~ s2, s2)
      diffContains(s1, s2, w)
    }
    disjointSubsetOf(s1 &~ s2, s2, s2 &~ s1)
  }.ensuring(
    (s1 &~ s2).disjoint(s2) &&
      (s1 &~ s2).disjoint(s2 &~ s1)
  )

  /** The difference of a set and a second one is equal to the difference of the former and the intersection of both
    */
  @pure
  @opaque
  def diffIntersection[T](s1: Set[T], s2: Set[T]): Unit = {
    if ((s1 &~ s2) =/= (s1 &~ (s1 & s2))) {
      val w = notEqualsWitness(s1 &~ s2, s1 &~ (s1 & s2))
      diffContains(s1, s2, w)
      diffContains(s1, s1 & s2, w)
      intersectContains(s1, s2, w)
    }
  }.ensuring(
    (s1 &~ s2) === (s1 &~ (s1 & s2))
  )

  @pure @opaque
  def diffUnionDef[T](s1: Set[T], s2: Set[T]): Unit = {
    if (s1 =/= (s1 &~ s2) ++ (s1 & s2)) {
      val w = notEqualsWitness(s1, (s1 &~ s2) ++ (s1 & s2))
      unionContains(s1 &~ s2, s1 & s2, w)
      diffContains(s1, s2, w)
      intersectContains(s1, s2, w)
    }
    diffIntersection(s1, s2)
    unionEqualsLeft(s1 &~ s2, s1 &~ (s1 & s2), s1 & s2)
    equalsTransitivity(s1, (s1 &~ s2) ++ (s1 & s2), (s1 &~ (s1 & s2)) ++ (s1 & s2))
  }.ensuring(
    (s1 === (s1 &~ s2) ++ (s1 & s2)) &&
      (s1 === (s1 &~ (s1 & s2)) ++ (s1 & s2))
  )

  /** The size of the difference between two sets is smaller than the size of the first one. It is actually equal to the
    * size of the first one minus the size of the intersection of both.
    */
  @pure
  @opaque
  def diffSize[T](s1: Set[T], s2: Set[T]): Unit = {
    diffDisjoint(s1, s1 & s2)
    unionDisjointSize(s1 &~ (s1 & s2), s1 & s2)
    diffUnionDef(s1, s2)
    sizeEquals(s1, (s1 &~ (s1 & s2)) ++ (s1 & s2))
    diffIntersection(s1, s2)
    sizeEquals(s1 &~ s2, s1 &~ (s1 & s2))
    sizePositive(s1 & s2)
  }.ensuring(
    ((s1 &~ s2).size == s1.size - (s1 & s2).size) &&
      ((s1 &~ s2).size <= s1.size)
  )

  /** A set is disjoint to an other one if and only if the difference of the first and the second one is equal to the
    * former set.
    */
  @pure @opaque
  def diffEquals[T](s1: Set[T], s2: Set[T]): Unit = {
    if (s1.disjoint(s2) & ((s1 &~ s2) =/= s1)) {
      val w = notEqualsWitness(s1 &~ s2, s1)
      diffContains(s1, s2, w)
      disjointContains(s1, s2, w)
    }
    if (!s1.disjoint(s2) & ((s1 &~ s2) === s1)) {
      val w = notDisjointWitness(s1, s2)
      equalsContains(s1 &~ s2, s1, w)
      diffContains(s1, s2, w)
    }
  }.ensuring(((s1 &~ s2) === s1) == s1.disjoint(s2))

  /** A set minus a value contains an element if and only if the set contains this element and it is different from the value
    */
  @pure
  @opaque
  def removeContains[T](s: Set[T], r: T, e: T): Unit = {
    unfold(s.remove(r))
    diffContains(s, Set(r), e)
    singletonContains(r, e)
  }.ensuring((s - r).contains(e) == (s.contains(e) && r != e))

  /** A set minus a value does not contain this value
    */
  @pure
  @opaque
  def removeContains[T](s: Set[T], r: T): Unit = {
    removeContains(s, r, r)
  }.ensuring(!(s - r).contains(r))

  /** If a set is subset of an other then removing an element on both sides preserves this relationship.
    */
  @pure
  @opaque
  def removeSubsetOf[T](s1: Set[T], s2: Set[T], r: T): Unit = {
    require(s1.subsetOf(s2))
    unfold(s1.remove(r))
    unfold(s2.remove(r))
    diffSubsetOf(s1, s2, Set(r))
  }.ensuring((s1 - r).subsetOf(s2 - r))

  /** If a set are equal then removing an element on both sides preserves this relationship.
    */
  @pure
  @opaque
  def removeEquals[T](s1: Set[T], s2: Set[T], r: T): Unit = {
    require(s1 === s2)
    unfold(s1.remove(r))
    unfold(s2.remove(r))
    diffEquals(s1, s2, Set(r))
  }.ensuring((s1 - r) === (s2 - r))

  /** --------------------------------------------------------------------------------------------------------------- *
    * ----------------------------------------------------SYMDIFF----------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If the symmetric difference of two sets contains an element then the element cannot belong to the two sets at the
    * same time.
    */
  @pure
  @opaque
  def symDiffContains[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    unfold(s1.symDiff(s2))
    diffContains(s1, s2, e)
    diffContains(s2, s1, e)
    unionContains(s1.diff(s2), s2.diff(s1), e)
  }.ensuring(
    (s1.symDiff(s2)).contains(e) ==
      ((s1.contains(e) && !s2.contains(e)) || (!s1.contains(e) && s2.contains(e)))
  )

  /** The symmetric difference of two sets is empty if and only if they are equal
    */
  @pure
  @opaque
  def symDiffIsEmpty[T](s1: Set[T], s2: Set[T], f: T => Boolean): Unit = {
    unfold(s1.symDiff(s2))
    diffIsEmpty(s1, s2)
    diffIsEmpty(s2, s1)
    isEmptyUnion(s1.diff(s2), s2.diff(s1))
  }.ensuring((s1.symDiff(s2)).isEmpty == (s1 === s2))

  /** Symmetric difference is a commutative operation.
    */
  @pure @opaque
  def symDiffCommutativity[T](s1: Set[T], s2: Set[T]): Unit = {
    unfold(s1.symDiff(s2))
    unfold(s2.symDiff(s1))
    unionCommutativity(s1.diff(s2), s2.diff(s1))
  }.ensuring(s1.symDiff(s2) === s2.symDiff(s1))

  @pure @opaque
  def symDiffEquals[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    unfold(s1.symDiff(s3))
    unfold(s2.symDiff(s3))
    unfold(s1.symDiff(s2))

    diffEquals(s1, s2, s3)
    diffEquals(s3, s1, s2)
    diffEquals(s2, s3, s1)
    if (s1 === s2) {
      unionEquals(s1 &~ s3, s2 &~ s3, s3 &~ s1, s3 &~ s2)
    }
    if (s2 === s3) {
      unionEquals(s1 &~ s2, s1 &~ s3, s2 &~ s1, s3 &~ s1)
    }
  }.ensuring(
    ((s1 === s2) ==> (s1.symDiff(s3) === s2.symDiff(s3))) &&
      ((s2 === s3) ==> (s1.symDiff(s2) === s1.symDiff(s3)))
  )

  /** If two pairs of set are equal then their symmetric differences are also equal
    */
  @pure
  @opaque
  def symDiffEquals[T](s1: Set[T], s2: Set[T], s3: Set[T], s4: Set[T]): Unit = {
    require(s1 === s2)
    require(s3 === s4)
    symDiffEquals(s1, s2, s3)
    symDiffEquals(s2, s4, s3)
    equalsTransitivity(s1.symDiff(s3), s2.symDiff(s3), s2.symDiff(s4))
  }.ensuring(s1.symDiff(s3) === s2.symDiff(s4))

  /** The size of the symmetric difference of two sets is smaller than the sum of the sizes.
    * It is actually equal to the sum of the sizes minus twice the size of the intersection.
    */
  @pure @opaque
  def symDiffSize[T](s1: Set[T], s2: Set[T]): Unit = {
    unfold(s1.symDiff(s2))
    diffSize(s1, s2)
    diffSize(s2, s1)
    diffDisjoint(s1, s2)
    unionDisjointSize(s1 &~ s2, s2 &~ s1)
    intersectCommutativity(s1, s2)
    sizeEquals(s1 & s2, s2 & s1)
    assert((s1.symDiff(s2).size == s1.size + s2.size - 2 * (s1 & s2).size))
  }.ensuring(
    (s1.symDiff(s2).size == s1.size + s2.size - 2 * (s1 & s2).size) &&
      (s1.symDiff(s2).size <= s1.size + s2.size)
  )

  /** --------------------------------------------------------------------------------------------------------------- *
    * --------------------------------------------------INTERSECT----------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** The intersection of two sets contains an element if and only if both sets contain it.
    */
  @pure @opaque
  def intersectContains[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    unfold(s1.intersect(s2))
    unionContains(s1, s2, e)
    symDiffContains(s1, s2, e)
    diffContains(s1 ++ s2, s1.symDiff(s2), e)
  }.ensuring((s1 & s2).contains(e) == (s1.contains(e) && s2.contains(e)))

  /** If a set is subset of another one, then its intersection with a third set is also a subset of the intersection of
    * the second and the third one.
    *
    * ∀s1,s2,s3. s1 ⊆ s2 => s1 ∩ s3 ⊆ s2 ∩ s3
    */
  @pure @opaque
  def intersectSubsetOfLeft[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    if (s1.subsetOf(s2) && !(s1 & s3).subsetOf(s2 & s3)) {
      val w = notSubsetOfWitness(s1 & s3, s2 & s3)
      intersectContains(s1, s3, w)
      intersectContains(s2, s3, w)
      subsetOfContains(s1, s2, w)
    }
  }.ensuring((s1.subsetOf(s2)) ==> (s1 & s3).subsetOf(s2 & s3))

  /** If a set is subset of another one, then its intersection with a third set is also a subset of the intersection of
    * the second and the third one.
    *
    * ∀s1,s2,s3. s2 ⊆ s3 => s1 ∩ s2 ⊆ s1 ∩ s3
    */
  @pure
  @opaque
  def intersectSubsetOfRight[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    if (s2.subsetOf(s3) && !(s1 & s2).subsetOf(s1 & s3)) {
      val w = notSubsetOfWitness(s1 & s2, s1 & s3)
      intersectContains(s1, s2, w)
      intersectContains(s1, s3, w)
      subsetOfContains(s2, s3, w)
    }

  }.ensuring(s2.subsetOf(s3) ==> (s1 & s2).subsetOf(s1 & s3))

  /** A set is subset of the intersection of two sets if and only if it is subset of both sets.
    */
  @pure
  @opaque
  def intersectSubsetOf[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    if (s1.subsetOf(s2 & s3)) {
      if (!s1.subsetOf(s2)) {
        val w = notSubsetOfWitness(s1, s2)
        subsetOfContains(s1, s2 & s3, w)
        intersectContains(s2, s3, w)
      }
      if (!s1.subsetOf(s3)) {
        val w = notSubsetOfWitness(s1, s3)
        subsetOfContains(s1, s2 & s3, w)
        intersectContains(s2, s3, w)
      }
    }

    if (s1.subsetOf(s2) && s1.subsetOf(s3) && !s1.subsetOf(s2 & s3)) {
      val w = notSubsetOfWitness(s1, s2 & s3)
      subsetOfContains(s1, s2, w)
      subsetOfContains(s1, s3, w)
      intersectContains(s2, s3, w)
      assert(false)
    }
  }.ensuring(s1.subsetOf(s2 & s3) == (s1.subsetOf(s2) && s1.subsetOf(s3)))

  /** The intersection of two sets is subset of each of the sets.
    */
  @pure
  @opaque
  def intersectSubsetOf[T](s1: Set[T], s2: Set[T]): Unit = {
    if (!(s1 & s2).subsetOf(s1)) {
      val w = notSubsetOfWitness(s1 & s2, s1)
      intersectContains(s1, s2, w)
    }
    if (!(s1 & s2).subsetOf(s2)) {
      val w = notSubsetOfWitness(s1 & s2, s2)
      intersectContains(s1, s2, w)
    }
  }.ensuring(
    (s1 & s2).subsetOf(s1) && (s1 & s2).subsetOf(s2)
  )

  @pure
  @opaque
  def intersectSubsetOf[T](s1: Set[T], s2: Set[T], s3: Set[T], s4: Set[T]): Unit = {
    require(s1.subsetOf(s2))
    require(s3.subsetOf(s4))
    intersectSubsetOfLeft(s1, s2, s3)
    intersectSubsetOfRight(s2, s3, s4)
    subsetOfTransitivity(s1 & s3, s2 & s3, s2 & s4)
  }.ensuring((s1 & s3).subsetOf(s2 & s4))

  /** If two sets are equal then their intersection with a third set is also equal
    */
  @pure
  @opaque
  def intersectEquals[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    intersectSubsetOfRight(s1, s2, s3)
    intersectSubsetOfLeft(s1, s2, s3)
    intersectSubsetOfLeft(s2, s1, s3)
    intersectSubsetOfRight(s1, s3, s2)
  }.ensuring(
    ((s1 === s2) ==> ((s1 & s3) === (s2 & s3))) &&
      ((s2 === s3) ==> ((s1 & s2) === (s1 & s3)))
  )

  /** If two pairs of sets are equal then their intersection is also equal.
    */
  @pure
  @opaque
  def intersectEquals[T](s1: Set[T], s2: Set[T], s3: Set[T], s4: Set[T]): Unit = {
    require(s1 === s2)
    require(s3 === s4)
    intersectSubsetOf(s1, s2, s3, s4)
    intersectSubsetOf(s2, s1, s4, s3)
  }.ensuring((s1 & s3) === (s2 & s4))

  /** Interesection is commutative.
    *
    * ∀s1,s2. s1 ∩ s2 = s2 ∩ s1
    */
  @pure @opaque
  def intersectCommutativity[T](s1: Set[T], s2: Set[T]): Unit = {
    unfold(s1.intersect(s2))
    unfold(s2.intersect(s1))
    symDiffCommutativity(s1, s2)
    unionCommutativity(s1, s2)
    diffEquals(s1 ++ s2, s2 ++ s1, s1.symDiff(s2), s2.symDiff(s1))
  }.ensuring((s1 & s2) === (s2 & s1))

  /** The union of two sets is equal to the union of their intersection and their symmetric difference.
    *
    * ∀s1,s2. |s1 ∩ s2| ≤ |s1| /\ |s1 ∩ s2| ≤ |s2|
    */
  @pure
  @opaque
  def intersectSize[T](s1: Set[T], s2: Set[T]): Unit = {
    unionSize(s1, s2)
  }.ensuring(
    (s1 & s2).size <= s1.size &&
      (s1 & s2).size <= s2.size
  )

  /** The union of two sets is equal to the union of their intersection and their symmetric difference.
    *
    * ∀s1,s2. s1 ∪ s2 = (s1 Δ s2) ∪ (s1 ∩ s2)
    */
  @pure
  @opaque
  def unionIntersectSymDiffDef[T](s1: Set[T], s2: Set[T]): Unit = {
    if ((s1 ++ s2) =/= (s1.symDiff(s2) ++ (s1 & s2))) {
      val w = notEqualsWitness(s1 ++ s2, s1.symDiff(s2) ++ (s1 & s2))
      unionContains(s1, s2, w)
      unionContains(s1.symDiff(s2), s1 & s2, w)
      intersectContains(s1, s2, w)
      symDiffContains(s1, s2, w)
    }
  }.ensuring(
    (s1 ++ s2) === (s1.symDiff(s2) ++ (s1 & s2))
  )

  /** The symmetric difference between two sets is disjoint from the intersection.
    */
  @pure @opaque
  def intersectDisjointSymDiff[T](s1: Set[T], s2: Set[T]): Unit = {
    if (!s1.symDiff(s2).disjoint(s1 & s2)) {
      val w = notDisjointWitness(s1.symDiff(s2), s1 & s2)
      symDiffContains(s1, s2, w)
      intersectContains(s1, s2, w)
    }
    disjointSymmetry(s1.symDiff(s2), s1 & s2)
  }.ensuring(
    s1.symDiff(s2).disjoint(s1 & s2) &&
      (s1 & s2).disjoint(s1.symDiff(s2))
  )

  /** Alternative definition of the subset relation.
    *
    * A set is subset of another one if and only if the intersection of both is equal to the former.
    */
  @pure
  @opaque
  def intersectAltDefinition[T](s1: Set[T], s2: Set[T]): Unit = {
    intersectSubsetOf(s1, s1, s2)
    subsetOfReflexivity(s1)
    intersectSubsetOf(s1, s2)
    intersectCommutativity(s1, s2)
    equalsTransitivityStrong(s1, s2 & s1, s1 & s2)
  }.ensuring(
    (s1.subsetOf(s2) == ((s1 & s2) === s1)) &&
      (s1.subsetOf(s2) == ((s2 & s1) === s1))
  )

  /** If two sets are not disjoint then we can exhibit an element such that both contain it.
    */
  @pure @opaque
  def notDisjointWitness[T](s1: Set[T], s2: Set[T]): T = {
    require(!s1.disjoint(s2))
    unfold(s1.disjoint(s2))
    val w = notEmptyWitness(s1.intersect(s2))
    intersectContains(s1, s2, w)
    w
  }.ensuring(e => s1.contains(e) && s2.contains(e))

  /** Two disjoint sets cannot contain the same element.
    */
  @pure @opaque
  def disjointContains[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    require(s1.disjoint(s2))
    unfold(s1.disjoint(s2))
    isEmptyContains(s1.intersect(s2), e)
    intersectContains(s1, s2, e)
  }.ensuring(!s1.contains(e) || !s2.contains(e))
  @pure @opaque
  def disjointSubsetOf[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    if (s3.subsetOf(s2) && s1.disjoint(s2) && !s1.disjoint(s3)) {
      val w = notDisjointWitness(s1, s3)
      disjointContains(s1, s2, w)
      subsetOfContains(s3, s2, w)
    }
    if (s2.subsetOf(s1) && s1.disjoint(s3) && !s2.disjoint(s3)) {
      val w = notDisjointWitness(s2, s3)
      disjointContains(s1, s3, w)
      subsetOfContains(s2, s1, w)
    }
  }.ensuring(
    ((s3.subsetOf(s2) && s1.disjoint(s2)) ==> s1.disjoint(s3)) &&
      ((s2.subsetOf(s1) && s1.disjoint(s3)) ==> s2.disjoint(s3))
  )

  /** If two sets are equal then the first is disjoint to a third set if and only if the second is also disjoint to it.
    */
  @pure
  @opaque
  def disjointEquals[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    disjointSubsetOf(s1, s2, s3)
    disjointSubsetOf(s1, s3, s2)
    disjointSubsetOf(s2, s1, s3)
  }.ensuring(
    ((s2 === s3) ==> (s1.disjoint(s2) == s1.disjoint(s3))) &&
      ((s1 === s2) ==> (s1.disjoint(s3) == s2.disjoint(s3)))
  )

  /** Disjointness is a symmetric relation.
    */
  @pure
  @opaque
  def disjointSymmetry[T](s1: Set[T], s2: Set[T]): Unit = {
    unfold(s1.disjoint(s2))
    unfold(s2.disjoint(s1))
    intersectCommutativity(s1, s2)
    isEmptyEquals(s1.intersect(s2), s2.intersect(s1))
  }.ensuring(
    s1.disjoint(s2) == s2.disjoint(s1)
  )

  /** A set is disjoint with the union of two other sets, if and only it is disjoint with both.
    */
  @pure @opaque
  def disjointUnionRight[T](s1: Set[T], s2: Set[T], s3: Set[T]): Unit = {
    if (s1.disjoint(s2 ++ s3)) {
      if (!s1.disjoint(s2)) {
        val w = notDisjointWitness(s1, s2)
        disjointContains(s1, s2 ++ s3, w)
        unionContains(s2, s3, w)
      }
      if (!s1.disjoint(s3)) {
        val w = notDisjointWitness(s1, s3)
        disjointContains(s1, s2 ++ s3, w)
        unionContains(s2, s3, w)
      }
    }
    if (s1.disjoint(s2) && s1.disjoint(s3)) {
      if (!s1.disjoint(s2 ++ s3)) {
        val w = notDisjointWitness(s1, s2 ++ s3)
        disjointContains(s1, s2, w)
        disjointContains(s1, s3, w)
        unionContains(s2, s3, w)
      }
    }
  }.ensuring(
    s1.disjoint(s2 ++ s3) == (s1.disjoint(s2) && s1.disjoint(s3))
  )

  /** A set is disjoint with an other set which an element has been added to if and only if it is disjoint to set and
    * does not contain the element.
    */
  @pure
  @opaque
  def disjointIncl[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    unfold(s2.incl(e))
    disjointUnionRight(s1, s2, Set(e))
    disjointSingleton(s1, e)
  }.ensuring(
    s1.disjoint(s2 + e) == (s1.disjoint(s2) && !s1.contains(e))
  )

  /** A set is disjoint with an other set which an element has been added to if and only if it is disjoint to set and
    * does not contain the element.
    */
  @pure
  @opaque
  def disjointInclRev[T](s1: Set[T], s2: Set[T], e: T): Unit = {
    disjointIncl(s2, s1, e)
    disjointSymmetry(s1 + e, s2)
    disjointSymmetry(s1, s2)
  }.ensuring(
    (s1 + e).disjoint(s2) == (s1.disjoint(s2) && !s2.contains(e))
  )

  /** --------------------------------------------------------------------------------------------------------------- *
    * ------------------------------------------------------MAP------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If a set contains an element, then the result after a map operation contains the element on which the function
    * has been applied.
    */
  @pure
  @opaque
  def mapContains[T, V](s: Set[T], f: T => V, e: T): Unit = {
    require(s.contains(e))
    unfold(s.contains)
    unfold(s.map[V](f).contains)

    val eqe: T => Boolean = _ == e
    val eqfe: V => Boolean = _ == f(e)

    forallNotExists(s, eqe)
    forallNotExists(s.map(f), eqfe)
    forallMap(s, f, !eqfe(_))

    val g: T => Boolean = f andThen (!eqfe(_))

    if (!s.map[V](f).contains(f(e)) & s.contains(e)) {
      forallContains(s, g, e)
    }
  }.ensuring(s.map[V](f).contains(f(e)))

  /** If a set after a map contains an element, then we can exhibit an element in the original set such that applying
    * the function to the latter gives the former.
    */
  @pure
  @opaque
  def mapContainsWitness[T, V](s: Set[T], f: T => V, e: V): T = {
    require(s.map[V](f).contains(e))

    if (s.forall(f andThen (_ != e))) {
      forallMap(s, f, _ != e)
      forallContains(s.map[V](f), _ != e, e)
    }
    notForallWitness(s, f andThen (_ != e))
  }.ensuring(res => (f(res) == e) && s.contains(res))

  /** If a set is a subset of an other then applying a map operations, does not change the subset relation.
    */
  @pure
  @opaque
  def mapSubsetOf[T, V](s1: Set[T], s2: Set[T], f: T => V): Unit = {
    require(s1.subsetOf(s2))
    if (!s1.map[V](f).subsetOf(s2.map[V](f))) {
      val w = notSubsetOfWitness(s1.map[V](f), s2.map[V](f))
      val e = mapContainsWitness(s1, f, w)
      subsetOfContains(s1, s2, e)
      mapContains(s2, f, e)
    }
  }.ensuring(s1.map[V](f).subsetOf(s2.map[V](f)))

  /** If two sets are equal then their map wrt a function is equal as well.
    */
  @pure
  @opaque
  def mapEquals[T, V](s1: Set[T], s2: Set[T], f: T => V): Unit = {
    require(s1 === s2)
    mapSubsetOf(s1, s2, f)
    mapSubsetOf(s2, s1, f)
  }.ensuring(s1.map[V](f) === s2.map[V](f))

  @pure @opaque @inlineOnce
  def mapExists[T, V](s: Set[T], f: T => V, p: V => Boolean): Unit = {
    forallNotExists(s.map[V](f), p)
    forallNotExists(s, f andThen p)

    val nfp: T => Boolean = x => !(f andThen p)(x)
    val np: V => Boolean = x => !p(x)

    if (s.map[V](f).forall(np) && !s.forall(nfp)) {
      val w = notForallWitness(s, nfp)
      mapContains(s, f, w)
      forallContains(s.map[V](f), np, f(w))
    }
    if (!s.map[V](f).forall(np) && s.forall(nfp)) {
      val w2 = notForallWitness(s.map[V](f), np)
      val w = mapContainsWitness(s, f, w2)
      forallContains(s, nfp, w)
    }
  }.ensuring(s.map[V](f).exists(p) == s.exists(f andThen p))

  /** The map of an union is the union of the maps.
    */
  @pure
  @opaque
  def mapUnion[T, V](s1: Set[T], s2: Set[T], f: T => V): Unit = {
    if ((s1 ++ s2).map[V](f) =/= (s1.map[V](f) ++ s2.map[V](f))) {
      val w = notEqualsWitness((s1 ++ s2).map[V](f), s1.map[V](f) ++ s2.map[V](f))
      if ((s1 ++ s2).map[V](f).contains(w)) {
        val v = mapContainsWitness(s1 ++ s2, f, w)
        unionContains(s1, s2, v)
        if (s1.contains(v)) {
          mapContains(s1, f, v)
        } else {
          mapContains(s2, f, v)
        }
        unionContains(s1.map[V](f), s2.map[V](f), f(v))
      }
      if ((s1.map[V](f) ++ s2.map[V](f)).contains(w)) {
        unionContains(s1.map[V](f), s2.map[V](f), w)
        if (s1.map[V](f).contains(w)) {
          val v1 = mapContainsWitness(s1, f, w)
          unionContains(s1, s2, v1)
          mapContains(s1 ++ s2, f, v1)
        } else {
          val v2 = mapContainsWitness(s2, f, w)
          unionContains(s1, s2, v2)
          mapContains(s1 ++ s2, f, v2)
        }
      }
    }
  }.ensuring((s1 ++ s2).map[V](f) === (s1.map[V](f) ++ s2.map[V](f)))

  /** Doing a map on a singleton is the singleton of the function applied to the element
    */
  @pure
  @opaque
  def mapSingleton[T, V](e: T, f: T => V): Unit = {
    if (Set(e).map[V](f) =/= Set(f(e))) {
      val w: V = notEqualsWitness(Set(e).map[V](f), Set(f(e)))
      if (Set(e).map[V](f).contains(w)) {
        val v = mapContainsWitness(Set(e), f, w)
        singletonContains(e, v)
        singletonContains(f(e), w)
      }
      if (Set[V](f(e)).contains(w)) {
        singletonContains(f(e), w)
        singletonContains(e)
        mapContains(Set(e), f, e)
      }
    }
  }.ensuring(Set(e).map(f) === Set(f(e)))

  /** Doing a map operation on a set and an element is the same as mapping the set only and then adding the element
    * on which the function has been applied.
    */
  @pure
  @opaque
  def mapIncl[T, V](s: Set[T], e: T, f: T => V): Unit = {
    unfold(s.incl(e))
    unfold(s.map[V](f).incl(f(e)))
    mapUnion(s, Set(e), f)
    mapSingleton(e, f)
    unionEqualsRight(s.map[V](f), Set(e).map[V](f), Set(f(e)))
    equalsTransitivity((s + e).map[V](f), s.map[V](f) ++ Set(e).map[V](f), s.map[V](f) ++ Set(f(e)))

  }.ensuring((s + e).map[V](f) === (s.map[V](f) + f(e)))

}

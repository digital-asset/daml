// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import stainless.lang.{unfold, None, Some, BooleanDecorations}
import stainless.annotation._
import scala.annotation.targetName
import MapAxioms._
import SetProperties._

object MapProperties {

  /** --------------------------------------------------------------------------------------------------------------- *
    * -------------------------------------------------EMPTY---------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** The empty map does not contain any element
    */
  @pure
  @opaque
  def emptyContains[K, V](k: K): Unit = {
    emptyGet[K, V](k)
    unfold(Map.empty[K, V].contains)
  }.ensuring(!Map.empty[K, V].contains(k))

  /** The empty map is submap of any other map.
    */
  @pure
  @opaque
  def emptySubmapOf[K, V](m: Map[K, V]): Unit = {
    if (!Map.empty[K, V].submapOf(m)) {
      val k = notSubmapOfWitness(Map.empty[K, V], m)
      emptyContains[K, V](k)
    }
  }.ensuring(Map.empty[K, V].submapOf(m))

  /** The set of values of the empty map is empty.
    */
  @pure @opaque
  def emptyValues[K, V]: Unit = {
    if (Map.empty[K, V].values =/= Set.empty[V]) {
      val w = SetProperties.notEqualsWitness(Map.empty[K, V].values, Set.empty[V])
      SetProperties.emptyContains(w)
      if (Map.empty[K, V].values.contains(w)) {
        val k = valuesWitness(Map.empty[K, V], w)
        emptyGet[K, V](k)
      }
    }
    unfold(Map.empty[K, V].values.isEmpty)
  }.ensuring(Map.empty[K, V].values.isEmpty)

  /** The keyset of the empty map is empty.
    */
  @pure
  @opaque
  def emptyKeySet[K, V]: Unit = {
    unfold(Map.empty[K, V].keySet)
    emptyValues[K, V]
    preimageIsEmpty(Map.empty[K, V], Map.empty[K, V].values)
  }.ensuring(Map.empty[K, V].keySet.isEmpty)

  /** If a map is non empty we can exhibit a key it contains
    */
  @pure
  @opaque
  def notEmptyWitness[K, V](m: Map[K, V]): K = {
    require(m =/= Map.empty[K, V])
    val k = notEqualsWitness(m, Map.empty[K, V])
    unfold(m.contains)
    unfold(Map.empty[K, V].contains)
    emptyContains[K, V](k)
    k
  }.ensuring(k => m.contains(k))

  /** --------------------------------------------------------------------------------------------------------------- *
    * -------------------------------------------------CONCAT--------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** The empty map is a neutral element wrt concatenation.
    */
  @pure @opaque
  def concatEmpty[K, V](m: Map[K, V]): Unit = {
    if (Map.empty[K, V] ++ m =/= m) {
      val k = notEqualsWitness(Map.empty[K, V] ++ m, m)
      concatGet(Map.empty[K, V], m, k)
      emptyGet[K, V](k)
    }
    if (m ++ Map.empty[K, V] =/= m) {
      val k = notEqualsWitness(m ++ Map.empty[K, V], m)
      concatGet(m, Map.empty[K, V], k)
      emptyGet[K, V](k)
    }
  }.ensuring(
    Map.empty[K, V] ++ m === m &&
      m ++ Map.empty[K, V] === m
  )

  /** The concatenation of two maps contains a key if and only if one of the two contains it.
    */
  @pure @opaque
  def concatContains[K, V](m1: Map[K, V], m2: Map[K, V], k: K): Unit = {
    unfold((m1 ++ m2).contains)
    unfold(m1.contains)
    unfold(m2.contains)
    concatGet(m1, m2, k)
  }.ensuring((m1 ++ m2).contains(k) == (m1.contains(k) || m2.contains(k)))

  /** The keyset of the concatenation of two maps is the union of both keysets.
    */
  @pure @opaque
  def concatKeySet[K, V](m1: Map[K, V], m2: Map[K, V]): Unit = {
    val unionKeySet = (m1 ++ m2).keySet
    val keySetUnion = m1.keySet ++ m2.keySet

    if (unionKeySet =/= keySetUnion) {
      val d = SetProperties.notEqualsWitness(unionKeySet, keySetUnion)
      unionContains(m1.keySet, m2.keySet, d)
      keySetContains(m1 ++ m2, d)
      concatContains(m1, m2, d)
      keySetContains(m1, d)
      keySetContains(m2, d)
    }
  }.ensuring((m1 ++ m2).keySet === m1.keySet ++ m2.keySet)

  /** The set of values of the concatenation of two maps is a subset of the union of the values of both sets.
    */
  @pure
  @opaque
  def concatValues[K, V](m1: Map[K, V], m2: Map[K, V]): Unit = {
    val unionValues = (m1 ++ m2).values
    val valuesUnion = m1.values ++ m2.values

    if (!unionValues.subsetOf(valuesUnion)) {
      val d = SetProperties.notSubsetOfWitness(unionValues, valuesUnion)
      val k = valuesWitness(m1 ++ m2, d)
      concatGet(m1, m2, k)
      unfold(m1.contains)
      unfold(m2.contains)
      if (m1.get(k) == Some[V](d)) {
        valuesContains(m1, k)
      } else {
        valuesContains(m2, k)
      }
      unionContains(m1.values, m2.values, d)
    }
  }.ensuring((m1 ++ m2).values.subsetOf(m1.values ++ m2.values))

  /** If a map is a submap of an other one then concatenating on the right does not change the subset relationship.
    */
  @pure @opaque
  def concatSubmapOf[K, V](m11: Map[K, V], m12: Map[K, V], m2: Map[K, V]): Unit = {
    require(m11.submapOf(m12))
    if (!(m11 ++ m2).submapOf(m12 ++ m2)) {
      val w = notSubmapOfWitness(m11 ++ m2, m12 ++ m2)
      concatContains(m11, m2, w)
      unfold(m2.contains)
      if (m11.contains(w)) {
        submapOfGet(m11, m12, w)
      }
      concatGet(m11, m2, w)
      concatGet(m12, m2, w)
    }
  }.ensuring((m11 ++ m2).submapOf(m12 ++ m2))

  /** If two maps are equal then concatenating them with a third list maintain the equality.
    */
  @pure
  @opaque
  def concatEqualsRight[K, V](m1: Map[K, V], m21: Map[K, V], m22: Map[K, V]): Unit = {
    require(m21 === m22)
    if ((m1 ++ m21) =/= (m1 ++ m22)) {
      val w = notEqualsWitness(m1 ++ m21, m1 ++ m22)
      equalsGet(m21, m22, w)
      concatGet(m1, m21, w)
      concatGet(m1, m22, w)
    }
  }.ensuring((m1 ++ m21) === (m1 ++ m22))

  /** Any map is a submap of itself concatenated with an other map on the left.
    */
  @pure
  @opaque
  def concatSubmapOf[K, V](m1: Map[K, V], m2: Map[K, V]): Unit = {
    concatEmpty(m2)
    emptySubmapOf(m1)
    concatSubmapOf(Map.empty[K, V], m1, m2)
    equalsSubmapOfTransitivity(m2, Map.empty[K, V] ++ m2, m1 ++ m2)
  }.ensuring(m2.submapOf(m1 ++ m2))

  /** If the keySet of a map is a subset of the keyset of another one, the second map is equal to the concatenation
    * between the first and the second one.
    */
  @pure
  @opaque
  def concatSubsetOfEquals[K, V](m1: Map[K, V], m2: Map[K, V]): Unit = {
    require(m1.keySet.subsetOf(m2.keySet))
    concatSubmapOf(m1, m2)
    if (!(m1 ++ m2).submapOf(m2)) {
      val k = notSubmapOfWitness(m1 ++ m2, m2)
      keySetContains(m1, k)
      keySetContains(m2, k)
      concatGet(m1, m2, k)
      unfold(m1.contains)
      unfold(m2.contains)
      subsetOfContains(m1.keySet, m2.keySet, k)
    }
  }.ensuring(m2 === m1 ++ m2)

  /** Map concatenation is idempotent. That is any map is equal to itself concatenated to itself again.
    */
  @pure
  @opaque
  def concatIdempotence[K, V](m: Map[K, V]): Unit = {
    subsetOfReflexivity(m.keySet)
    concatSubsetOfEquals(m, m)
  }.ensuring(m === m ++ m)

  /** Map concatenation is an associative operation.
    */
  @pure
  @opaque
  def concatAssociativity[K, V](m1: Map[K, V], m2: Map[K, V], m3: Map[K, V]): Unit = {
    if (((m1 ++ m2) ++ m3) =/= (m1 ++ (m2 ++ m3))) {
      val k = notEqualsWitness((m1 ++ m2) ++ m3, m1 ++ (m2 ++ m3))
      concatGet(m1 ++ m2, m3, k)
      concatGet(m1, m2, k)
      concatGet(m1, m2 ++ m3, k)
      concatGet(m2, m3, k)
    }
  }.ensuring(((m1 ++ m2) ++ m3) === (m1 ++ (m2 ++ m3)))

  /** If two maps have disjoint keysets then their concatenation is commutative.
    */
  @pure
  @opaque
  def concatCommutativity[K, V](m1: Map[K, V], m2: Map[K, V]): Unit = {
    require(m1.keySet.disjoint(m2.keySet))
    if (m1 ++ m2 =/= m2 ++ m1) {
      val k = notEqualsWitness(m1 ++ m2, m2 ++ m1)
      concatGet(m1, m2, k)
      concatGet(m2, m1, k)
      keySetContains(m1, k)
      keySetContains(m2, k)
      disjointContains(m1.keySet, m2.keySet, k)
      unfold(m1.contains)
      unfold(m2.contains)
    }
  }.ensuring(m1 ++ m2 === m2 ++ m1)

  /** --------------------------------------------------------------------------------------------------------------- *
    * -------------------------------------------------SINGLETON------------------------------------------------------ *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** A singleton contains a key if and only if it is equal to its first argument
    */
  @pure @opaque
  def singletonContains[K, V](k: K, v: V, ks: K): Unit = {
    singletonGet(k, v, ks)
    unfold(Map[K, V](k -> v).contains)
  }.ensuring(Map[K, V](k -> v).contains(ks) == (k == ks))

  /** A singleton map contains the key of its unique pair.
    */
  @pure
  @opaque
  def singletonContains[K, V](k: K, v: V): Unit = {
    singletonContains(k, v, k)
  }.ensuring(Map[K, V](k -> v).contains(k))

  /** The values of a singelton is the singleton of its second argument.
    */
  @pure @opaque
  def singletonKeySet[K, V](k: K, v: V): Unit = {
    if (Map[K, V](k -> v).keySet =/= Set(k)) {
      val d = SetProperties.notEqualsWitness(Map[K, V](k -> v).keySet, Set(k))
      keySetContains(Map(k -> v), d)
      singletonContains(k, v, d)
      SetProperties.singletonContains(k, d)
    }
  }.ensuring(Map[K, V](k -> v).keySet === Set(k))

  /** The values of a singelton is the singleton of its second argument.
    */
  @pure
  @opaque
  def singletonValues[K, V](k: K, v: V): Unit = {
    if (Map[K, V](k -> v).values =/= Set[V](v)) {
      val d: V = SetProperties.notEqualsWitness(Map[K, V](k -> v).values, Set[V](v))
      SetProperties.singletonContains[V](v, d)

      if (Map[K, V](k -> v).values.contains(d)) {
        val ks = valuesWitness(Map(k -> v), d)
        singletonGet(k, v, ks)
      } else {
        singletonGet(k, v, k)
        unfold(Map[K, V](k -> v).contains)
        valuesContains(Map[K, V](k -> v), k)
      }
    }
  }.ensuring(Map[K, V](k -> v).values === Set[V](v))

  /** A singleton whose values has been mapped is equal to the singleton to which the function has been applied to the
    * second argument.
    */
  @pure
  @opaque
  @targetName("singletonMapValuesArgPair")
  def singletonMapValues[K, V, V2](k: K, v: V, f: K => V => V2): Unit = {
    if (Map(k -> v).mapValues[V2](f) =/= Map(k -> f(k)(v))) {
      val w = notEqualsWitness(Map(k -> v).mapValues[V2](f), Map(k -> f(k)(v)))
      singletonGet(k, v, w)
      MapAxioms.mapValuesGet(Map(k -> v), f, w)
      singletonGet(k, f(k)(v), w)
    }
  }.ensuring(Map(k -> v).mapValues[V2](f) === Map(k -> f(k)(v)))

  /** A singleton whose values has been mapped is equal to the singleton to which the function has been applied to the
    * second argument.
    */
  @pure
  @opaque
  def singletonMapValues[K, V, V2](k: K, v: V, f: V => V2): Unit = {
    singletonMapValues(k, v, (k: K) => f)
  }.ensuring(Map(k -> v).mapValues[V2](f) === Map(k -> f(v)))

  /** --------------------------------------------------------------------------------------------------------------- *
    * ---------------------------------------------------MAP---------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** The mapping of a map after a map operations on its value is the mapping of the original map on which the function
    * has been applied.
    */
  @pure
  @opaque
  def mapValuesGet[K, V, V2](m: Map[K, V], f: V => V2, k: K): Unit = {
    MapAxioms.mapValuesGet(m, (k: K) => f, k)
  }.ensuring(m.mapValues(f).get(k) == m.get(k).map(f))

  /** A map after a mapValues operation contains a key if and only if the original map also contains the key
    */
  @pure
  @opaque
  @targetName("mapValuesContainsArgPair")
  def mapValuesContains[K, V, V2](m: Map[K, V], f: K => V => V2, k: K): Unit = {
    MapAxioms.mapValuesGet(m, f, k)
    unfold(m.mapValues(f).contains)
    unfold(m.contains)
  }.ensuring(m.mapValues(f).contains(k) == m.contains(k))

  /** A map after a mapValues operation contains a key if and only if the original map also contains the key
    */
  @pure
  @opaque
  def mapValuesContains[K, V, V2](m: Map[K, V], f: V => V2, k: K): Unit = {
    mapValuesContains(m, (k: K) => f, k)
  }.ensuring(m.mapValues(f).contains(k) == m.contains(k))

  /** The keyset of a map after a map operations on its value as been performed is the same as the keyset of the original
    * map.
    */
  @pure
  @opaque
  @targetName("mapValuesKeySetArgPair")
  def mapValuesKeySet[K, V, V2](m: Map[K, V], f: K => V => V2): Unit = {
    if (m.mapValues(f).keySet =/= m.keySet) {
      val d = SetProperties.notEqualsWitness(m.mapValues(f).keySet, m.keySet)
      keySetContains(m.mapValues(f), d)
      keySetContains(m, d)
      mapValuesContains(m, f, d)
      Unreachable()
    }
  }.ensuring(m.mapValues(f).keySet === m.keySet)

  /** The key set after applying a map operation on values does not change the key set.
    */
  @pure
  @opaque
  def mapValuesKeySet[K, V, V2](m: Map[K, V], f: V => V2): Unit = {
    mapValuesKeySet(m, (k: K) => f)
  }.ensuring(m.mapValues(f).keySet === m.keySet)

  /** Applying a map values operation on a map twice is the same as applying map once with the composition of both functions.
    */
  @pure
  @opaque
  @inlineOnce
  def mapValuesAndThen[K, V, V2, V3](m: Map[K, V], f: V => V2, g: V2 => V3): Unit = {
    val mapMap = m.mapValues(f).mapValues(g)
    val mapAT = m.mapValues(f andThen g)
    if (mapMap =/= mapAT) {
      val w = notEqualsWitness(mapMap, mapAT)
      mapValuesGet(m.mapValues(f), g, w)
      mapValuesGet(m, f, w)
      mapValuesGet(m, f andThen g, w)
    }

  }.ensuring(m.mapValues(f).mapValues(g) === m.mapValues(f andThen g))

  /** If a map is submap of an other one then applying a map operation on their values does not change the relationship.
    */
  @pure @opaque
  def mapValuesSubmapOf[K, V, V2](m1: Map[K, V], m2: Map[K, V], f: K => V => V2): Unit = {
    require(m1.submapOf(m2))
    if (!m1.mapValues(f).submapOf(m2.mapValues(f))) {
      val w = notSubmapOfWitness(m1.mapValues(f), m2.mapValues(f))
      mapValuesContains(m1, f, w)
      submapOfGet(m1, m2, w)
      MapAxioms.mapValuesGet(m1, f, w)
      MapAxioms.mapValuesGet(m2, f, w)
    }
  }.ensuring(m1.mapValues(f).submapOf(m2.mapValues(f)))

  /** If a map is submap of an other one then applying a map operation on their values does not change the relationship.
    */
  @pure
  @opaque
  @targetName("mapValuesSubmapOfArgPair")
  def mapValuesSubmapOf[K, V, V2](m1: Map[K, V], m2: Map[K, V], f: V => V2): Unit = {
    require(m1.submapOf(m2))
    mapValuesSubmapOf(m1, m2, (k: K) => f)
  }.ensuring(m1.mapValues(f).submapOf(m2.mapValues(f)))

  /** If a map is equals to an other one then applying a map operation on their values does not change the relationship.
    */
  @pure
  @opaque
  def mapValuesEquals[K, V, V2](m1: Map[K, V], m2: Map[K, V], f: K => V => V2): Unit = {
    require(m1 === m2)
    mapValuesSubmapOf(m1, m2, f)
    mapValuesSubmapOf(m2, m1, f)
  }.ensuring(m1.mapValues(f) === m2.mapValues(f))

  /** If a map is equals to an other one then applying a map operation on their values does not change the relationship.
    */
  @pure
  @opaque
  @targetName("mapValuesEqualsArgPair")
  def mapValuesEquals[K, V, V2](m1: Map[K, V], m2: Map[K, V], f: V => V2): Unit = {
    require(m1 === m2)
    mapValuesSubmapOf(m1, m2, f)
    mapValuesSubmapOf(m2, m1, f)
  }.ensuring(m1.mapValues(f) === m2.mapValues(f))

  /** Applying a map operations on the values of the emtpy map gives the empty map
    */
  @pure
  @opaque
  def mapValuesEmpty[K, V, V2](f: K => V => V2): Unit = {
    if (Map.empty[K, V].mapValues(f) =/= Map.empty[K, V2]) {
      val k = notEqualsWitness(Map.empty[K, V].mapValues(f), Map.empty[K, V2])
      emptyGet[K, V2](k)
      emptyGet[K, V](k)
      MapAxioms.mapValuesGet(Map.empty[K, V], f, k)
    }
  }.ensuring(Map.empty[K, V].mapValues(f) === Map.empty[K, V2])

  /** Applying a map operations on the values of the emtpy map gives the empty map
    */
  @pure
  @opaque
  @targetName("mapValuesEmptyArgPair")
  def mapValuesEmpty[K, V, V2](f: V => V2): Unit = {
    mapValuesEmpty[K, V, V2]((k: K) => f)
  }.ensuring(Map.empty[K, V].mapValues(f) === Map.empty[K, V2])

  /** The set of values of a map after a map operation has been applied to its values is the set of values of the original map
    * mapped by the same function.
    */
  @pure
  @opaque
  def mapValuesValues[K, V, V2](m: Map[K, V], f: V => V2): Unit = {
    if (m.mapValues[V2](f).values =/= m.values.map[V2](f)) {
      val w: V2 = SetProperties.notEqualsWitness(m.mapValues[V2](f).values, m.values.map[V2](f))
      if (m.mapValues[V2](f).values.contains(w)) {
        val k: K = valuesWitness(m.mapValues[V2](f), w)
        MapProperties.mapValuesGet(m, f, k)
        unfold(m.contains)
        valuesContains(m, k)
        SetProperties.mapContains(m.values, f, m(k))
      } else {
        val v: V = SetProperties.mapContainsWitness(m.values, f, w)
        val k: K = valuesWitness(m, v)
        MapProperties.mapValuesGet(m, f, k)
        unfold(m.mapValues[V2](f).contains)
        valuesContains(m.mapValues[V2](f), k)
      }
    }
  }.ensuring(m.mapValues[V2](f).values === m.values.map(f))

  /** Mapping the values of a map concatenation is the same then mapping both maps and then concatenating them.
    */
  @pure
  @opaque
  @targetName("mapValuesConcatArgPair")
  def mapValuesConcat[K, V, V2](m1: Map[K, V], m2: Map[K, V], f: K => V => V2): Unit = {
    if ((m1 ++ m2).mapValues[V2](f) =/= (m1.mapValues[V2](f) ++ m2.mapValues[V2](f))) {
      val k =
        notEqualsWitness((m1 ++ m2).mapValues[V2](f), m1.mapValues[V2](f) ++ m2.mapValues[V2](f))
      MapAxioms.mapValuesGet(m1 ++ m2, f, k)
      concatGet(m1, m2, k)
      MapAxioms.mapValuesGet(m1, f, k)
      MapAxioms.mapValuesGet(m2, f, k)
      concatGet(m1.mapValues[V2](f), m2.mapValues[V2](f), k)
    }
  }.ensuring((m1 ++ m2).mapValues[V2](f) === (m1.mapValues[V2](f) ++ m2.mapValues[V2](f)))

  /** Mapping the values of a map concatenation is the same then mapping both maps and then concatenating them.
    */
  @pure
  @opaque
  def mapValuesConcat[K, V, V2](m1: Map[K, V], m2: Map[K, V], f: V => V2): Unit = {
    mapValuesConcat(m1, m2, (k: K) => f)
  }.ensuring((m1 ++ m2).mapValues[V2](f) === (m1.mapValues[V2](f) ++ m2.mapValues[V2](f)))

  /** Mapping the values of a map with an added pair is the same then mapping the maps and then applying the function
    * to the value in the pair and then adding it.
    */
  @pure
  @opaque
  @targetName("mapValuesUpdatedArgPair")
  def mapValuesUpdated[K, V, V2](m: Map[K, V], k: K, v: V, f: K => V => V2): Unit = {
    unfold(m.updated(k, v))
    unfold(m.mapValues[V2](f).updated(k, f(k)(v)))
    mapValuesConcat(m, Map(k -> v), f)
    singletonMapValues(k, v, f)
    concatEqualsRight(m.mapValues[V2](f), Map(k -> v).mapValues[V2](f), Map(k -> f(k)(v)))
    equalsTransitivity(
      m.updated(k, v).mapValues[V2](f),
      m.mapValues[V2](f) ++ Map(k -> v).mapValues[V2](f),
      m.mapValues[V2](f) ++ Map(k -> f(k)(v)),
    )
  }.ensuring(m.updated(k, v).mapValues[V2](f) === m.mapValues[V2](f).updated(k, f(k)(v)))

  /** Mapping the values of a map with an added pair is the same then mapping the maps and then applying the function
    * to the value in the pair and then adding it.
    */
  @pure
  @opaque
  def mapValuesUpdated[K, V, V2](m: Map[K, V], k: K, v: V, f: V => V2): Unit = {
    mapValuesUpdated(m, k, v, (k: K) => f)
  }.ensuring(m.updated(k, v).mapValues[V2](f) === m.mapValues[V2](f).updated(k, f(v)))

  /** --------------------------------------------------------------------------------------------------------------- *
    * -----------------------------------------------SUBMAP OF-------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If a map contains a key and is submap of another map then it also contains this key.
    */
  @pure
  @opaque
  def submapOfContains[K, V](m1: Map[K, V], m2: Map[K, V], k: K): Unit = {
    require(m1.submapOf(m2))
    require(m1.contains(k))
    submapOfGet(m1, m2, k)
    unfold(m1.contains)
    unfold(m2.contains)
  }.ensuring(m2.contains(k))

  /** Every map is submap of itself
    */
  @pure
  @opaque
  def submapOfReflexivity[K, V](m: Map[K, V]): Unit = {
    if (!m.submapOf(m)) {
      val w = notSubmapOfWitness(m, m)
      Unreachable()
    }
  }.ensuring(m.submapOf(m))

  /** Submap is a transitive relation.
    */
  @pure
  @opaque
  def submapOfTransitivity[K, V](m1: Map[K, V], m2: Map[K, V], m3: Map[K, V]): Unit = {
    require(m1.submapOf(m2))
    require(m2.submapOf(m3))
    if (!m1.submapOf(m3)) {
      val d = notSubmapOfWitness(m1, m3)
      submapOfContains(m1, m2, d)
      submapOfGet(m1, m2, d)
      submapOfGet(m2, m3, d)
      Unreachable()
    }
  }.ensuring(m1.submapOf(m3))

  /** If two maps are equal then the first is submap of a third map if and only if the second also is.
    */
  @pure
  @opaque
  def equalsSubmapOfTransitivity[K, V](m1: Map[K, V], m2: Map[K, V], m3: Map[K, V]): Unit = {
    require(m1 === m2)

    if (m1.submapOf(m3)) {
      submapOfTransitivity(m2, m1, m3)
    }
    if (m2.submapOf(m3)) {
      submapOfTransitivity(m1, m2, m3)
    }

  }.ensuring(
    m1.submapOf(m3) == m2.submapOf(m3)
  )

  /** If two maps are equal then the first is a supermap of a third map if and only if the second also is.
    */
  @pure
  @opaque
  def submapOfEqualsTransitivity[K, V](m1: Map[K, V], m2: Map[K, V], m3: Map[K, V]): Unit = {
    require(m2 === m3)

    if (m1.submapOf(m2)) {
      submapOfTransitivity(m1, m2, m3)
    }
    if (m1.submapOf(m3)) {
      submapOfTransitivity(m1, m3, m2)
    }

  }.ensuring(
    m1.submapOf(m2) == m1.submapOf(m3)
  )

  /** If a map is submap of another one then its keySets are also subset of each other.
    */
  @pure @opaque
  def submapOfKeySet[K, V](m1: Map[K, V], m2: Map[K, V]): Unit = {
    require(m1.submapOf(m2))

    if (!m1.keySet.subsetOf(m2.keySet)) {
      val w = SetProperties.notSubsetOfWitness(m1.keySet, m2.keySet)
      keySetContains(m1, w)
      keySetContains(m2, w)
      submapOfContains(m1, m2, w)
    }

  }.ensuring(m1.keySet.subsetOf(m2.keySet))

  /** --------------------------------------------------------------------------------------------------------------- *
    * -------------------------------------------------EQUALS--------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If two maps are equal, they contain an element if an only if the other one does as well.
    */
  @pure
  @opaque
  def equalsContains[K, V](m1: Map[K, V], m2: Map[K, V], k: K): Unit = {
    require(m1 === m2)
    if (m1.contains(k))
      submapOfContains(m1, m2, k)
    if (m2.contains(k))
      submapOfContains(m2, m1, k)
  }.ensuring(m1.contains(k) == m2.contains(k))

  /** If two maps are equal then the mapping associated to any of their key is equal as well.
    */
  @pure
  @opaque
  def equalsGet[K, V](m1: Map[K, V], m2: Map[K, V], k: K): Unit = {
    require(m1 === m2)
    equalsContains(m1, m2, k)
    unfold(m1.contains)
    unfold(m2.contains)
    if (m1.contains(k)) {
      submapOfGet(m1, m2, k)
      submapOfGet(m2, m1, k)
    }
  }.ensuring(m1.get(k) == m2.get(k))

  /** If two maps are not equal then we can exhibit a key such that they mapping differ.
    */
  @pure
  @opaque
  def notEqualsWitness[K, V](m1: Map[K, V], m2: Map[K, V]): K = {
    require(m1 =/= m2)
    if (!m1.submapOf(m2))
      notSubmapOfWitness(m1, m2)
    else
      notSubmapOfWitness(m2, m1)
  }.ensuring(res => m1.get(res) != m2.get(res))

  /** Two equal maps have the same keyset.
    */
  @pure @opaque
  def equalsKeySet[K, V](m1: Map[K, V], m2: Map[K, V]): Unit = {
    require(m1 === m2)
    val ks1 = m1.keySet
    val ks2 = m2.keySet
    if (ks1 =/= ks2) {
      val d = SetProperties.notEqualsWitness(ks1, ks2)
      keySetContains(m1, d)
      keySetContains(m2, d)
      equalsContains(m1, m2, d)
    }
  }.ensuring(m1.keySet === m2.keySet)

  /** Map equality is transitive.
    */
  @pure
  @opaque
  def equalsTransitivityStrong[K, V](m1: Map[K, V], m2: Map[K, V], m3: Map[K, V]): Unit = {
    require(m2 === m3)
    submapOfEqualsTransitivity(m1, m2, m3)
    equalsSubmapOfTransitivity(m2, m3, m1)
  }.ensuring((m1 === m2) == (m1 === m3))

  /** Map equality is transitive.
    */
  @pure
  @opaque
  def equalsTransitivity[K, V](m1: Map[K, V], m2: Map[K, V], m3: Map[K, V]): Unit = {
    require(m1 === m2)
    require(m2 === m3)
    equalsTransitivityStrong(m1, m2, m3)
  }.ensuring(m1 === m3)

  /** --------------------------------------------------------------------------------------------------------------- *
    * ------------------------------------------------KEY SET--------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** A map contains a key if and only if its key set contains the key as well.
    */
  @pure
  @opaque
  def keySetContains[K, V](m: Map[K, V], ks: K): Unit = {
    unfold(m.keySet)
    MapAxioms.preimageGet(m, m.values, ks)
    unfold(m.contains)
    if (m.contains(ks)) {
      valuesContains(m, ks)
    }
  }.ensuring(m.contains(ks) == m.keySet.contains(ks))

  /** If two maps have the same key set then one contains a key if and only if the other does as well.
    */
  @pure
  @opaque
  def equalsKeySetContains[K, V, V2](m1: Map[K, V], m2: Map[K, V2], k: K): Unit = {
    require(m1.keySet === m2.keySet)
    SetProperties.equalsContains(m1.keySet, m2.keySet, k)
    keySetContains(m1, k)
    keySetContains(m2, k)
  }.ensuring(m1.contains(k) == m2.contains(k))

  /** --------------------------------------------------------------------------------------------------------------- *
    * ---------------------------------------------------UPDATED------------------------------------------------------ *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** A map which has been updated with a new pair contains a key if and only if either the oiriginal map contains the
    * key or if it is equal to the first element of the pair.
    */
  @pure @opaque
  def updatedContains[K, V](m: Map[K, V], k: K, v: V, e: K): Unit = {
    keySetContains(m.updated(k, v), e)
    SetProperties.equalsContains(m.updated(k, v).keySet, m.keySet + k, e)
    keySetContains(m, e)
    inclContains(m.keySet, k, e)
  }.ensuring(m.updated(k, v).contains(e) == (m.contains(e) || k == e))

  /** If a pair is added to a map, then the mapping associated to a key other than the one in the pair is the same mapping
    * as in the original map.
    */
  @pure
  @opaque
  def updatedGet[K, V](m: Map[K, V], k: K, v: V, k2: K): Unit = {
    require(k != k2)
    unfold(m.updated(k, v))
    concatGet(m, Map(k -> v), k2)
    singletonGet(k, v, k2)
  }.ensuring(m.updated(k, v).get(k2) == m.get(k2))

  /** If a map does not contain the key of a pair, then adding this pair is equivalent to concatenating it on the left.
    */
  @pure
  @opaque
  def updatedCommutativity[K, V](m: Map[K, V], k: K, v: V): Unit = {
    require(!m.contains(k))
    unfold(m.updated(k, v))
    singletonKeySet(k, v)
    disjointEquals(m.keySet, Set(k), Map[K, V](k -> v).keySet)
    disjointSingleton(m.keySet, k)
    keySetContains(m, k)
    concatCommutativity(m, Map[K, V](k -> v))
  }.ensuring(m.updated(k, v) === Map[K, V](k -> v) ++ m)

  /** --------------------------------------------------------------------------------------------------------------- *
    * ------------------------------------------------VALUES-------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** The values of a map to which a mapping has been added are a subset of the valeus of the original map to which the
    * value of the pair has been added.
    */
  @pure
  @opaque
  def updatedValues[K, V](m: Map[K, V], k: K, v: V): Unit = {
    unfold(m.updated(k, v))
    concatValues(m, Map(k -> v))
    singletonValues(k, v)
    unionEqualsRight(m.values, Map(k -> v).values, Set(v))
    unfold(m.values + v)
    subsetOfTransitivity(m.updated(k, v).values, m.values ++ Map(k -> v).values, m.values ++ Set(v))
  }.ensuring(
    m.updated(k, v).values.subsetOf(m.values + v)
  )

  /** --------------------------------------------------------------------------------------------------------------- *
    * ------------------------------------------------PREIMAGE-------------------------------------------------------- *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If a map is submap of an other one then the preimage of the first wrt a set is subset of the preimage of the second.
    */
  @pure
  @opaque
  def preimageSubsetOf[K, V](m1: Map[K, V], m2: Map[K, V], s: Set[V]): Unit = {
    require(m1.submapOf(m2))
    if (!m1.preimage(s).subsetOf(m2.preimage(s))) {
      val k = notSubsetOfWitness(m1.preimage(s), m2.preimage(s))
      MapAxioms.preimageGet(m1, s, k)
      MapAxioms.preimageGet(m2, s, k)
      unfold(m1.contains)
      submapOfGet(m1, m2, k)
    }
  }.ensuring(m1.preimage(s).subsetOf(m2.preimage(s)))

  /** If two maps are equal then their preimages wrt a set are also equal.
    */
  @pure
  @opaque
  def preimageEquals[K, V](m1: Map[K, V], m2: Map[K, V], s: Set[V]): Unit = {
    require(m1 === m2)
    preimageSubsetOf(m1, m2, s)
    preimageSubsetOf(m2, m1, s)
  }.ensuring(m1.preimage(s) === m2.preimage(s))

  /** If a map is submap of an other one then the preimage of the first wrt a value is subset of the preimage of the
    * second.
    */
  @pure
  @opaque
  def preimageSubsetOf[K, V](m1: Map[K, V], m2: Map[K, V], s: V): Unit = {
    require(m1.submapOf(m2))
    unfold(m1.preimage(s))
    unfold(m2.preimage(s))
    preimageSubsetOf(m1, m2, Set(s))
  }.ensuring(m1.preimage(s).subsetOf(m2.preimage(s)))

  /** If two maps are equal then their preimages wrt an element are also equal.
    */
  @pure
  @opaque
  def preimageEquals[K, V](m1: Map[K, V], m2: Map[K, V], s: V): Unit = {
    require(m1 === m2)
    unfold(m1.preimage(s))
    unfold(m2.preimage(s))
    preimageEquals(m1, m2, Set(s))
  }.ensuring(m1.preimage(s) === m2.preimage(s))

  /** The preimage of any set wrt a map is a subset of the keyset of that map
    */
  @pure @opaque
  def preimageKeySet[K, V](m: Map[K, V], s: Set[V]): Unit = {
    if (!m.preimage(s).subsetOf(m.keySet)) {
      val k = SetProperties.notSubsetOfWitness(m.preimage(s), m.keySet)
      MapAxioms.preimageGet(m, s, k)
      keySetContains(m, k)
      unfold(m.contains)
    }
  }.ensuring(m.preimage(s).subsetOf(m.keySet))

  /** The preimage of a set wrt a map is empty if and only if this set is disjoint from the values of the map.
    * In particular, if the set is empty this is true.
    */
  @pure
  @opaque
  def preimageIsEmpty[K, V](m: Map[K, V], s: Set[V]): Unit = {
    if (m.preimage(s).isEmpty && !s.disjoint(m.values)) {
      val v = notDisjointWitness(s, m.values)
      val k = valuesWitness(m, v)
      MapAxioms.preimageGet(m, s, k)
      isEmptyContains(m.preimage(s), k)
    }
    if (!m.preimage(s).isEmpty && s.disjoint(m.values)) {
      val k = SetProperties.notEmptyWitness(m.preimage(s))
      unfold(m.contains)
      MapAxioms.preimageGet(m, s, k)
      disjointContains(s, m.values, m(k))
      valuesContains(m, k)
    }
    if (s.isEmpty) {
      disjointIsEmpty(s, m.values)
    }
  }.ensuring(
    (s.disjoint(m.values) == m.preimage(s).isEmpty) &&
      (s.isEmpty ==> m.preimage(s).isEmpty)
  )

  /** The preimage of a map with respect to a value contains a key if and only if the mapping associated to the key in
    * the map is equal to the value.
    */
  @pure
  @extern
  def preimageGet[K, V](m: Map[K, V], v: V, k: K): Unit = {}.ensuring(
    m.preimage(v).contains(k) == (m.get(k) == Some[V](v))
  )

  /** If the values of a map do not contain an element, then the preimage wrt that element is empty.
    */
  @pure
  @opaque
  def preimageIsEmpty[K, V](m: Map[K, V], v: V): Unit = {
    unfold(m.preimage(v))
    preimageIsEmpty(m, Set(v))
    SetProperties.disjointSingleton(m.values, v)
  }.ensuring(!m.values.contains(v) == m.preimage(v).isEmpty)

  /** If the preimage of a value is a singleton then any other key mapping to this value is equal to the key in the preimage.
    */
  @pure
  @opaque
  def preimageSingletonGet[K, V](m: Map[K, V], v: V, k: K, k2: K): Unit = {
    require(m.preimage(v) === Set[K](k))
    SetProperties.singletonSubsetOf(m.preimage(v), k)
    preimageGet(m, v, k)
    if (m.get(k2) == Some[V](v)) {
      preimageGet(m, v, k2)
      SetProperties.singletonContains(k, k2)
      SetProperties.equalsContains(m.preimage(v), Set(k), k2)
    }
  }.ensuring((m.get(k2) == Some[V](v)) == (k == k2))

  /** The preimage of a set wrt to a singleton is either the key if the mapping of the singleton is contained in the set
    * or the empty set.
    */
  @pure
  @opaque
  def preimageSingleton[K, V](k: K, v: V, s: Set[V]): Unit = {
    val m: Map[K, V] = Map(k -> v)
    if (m.preimage(s) =/= (if (s.contains(v)) Set(k) else Set.empty[K])) {
      val ks = SetProperties.notEqualsWitness[K](
        m.preimage(s),
        if (s.contains(v)) Set(k) else Set.empty[K],
      )
      MapAxioms.preimageGet(m, s, ks)
      SetProperties.emptyContains(ks)
      singletonGet(k, v, ks)
      SetProperties.singletonContains(k, ks)
    }
  }.ensuring(
    ((m: Map[K, V]) => m.preimage(s) === (if (s.contains(v)) Set(k) else Set.empty[K]))(
      Map[K, V](k -> v)
    )
  )

  /** The preimage of a value wrt to a singleton is either the key if the mapping of the singleton is equal to the value
    * or the empty set.
    */
  @pure
  @opaque
  def preimageSingleton[K, V](k: K, v: V, s: V): Unit = {
    unfold(Map(k -> v).preimage(s))
    SetProperties.singletonContains(s, v)
    preimageSingleton(k, v, Set(s))
  }.ensuring(Map(k -> v).preimage(s) === (if (s == v) Set(k) else Set.empty[K]))

  /** The preimage of a concatenation between two maps is a subset of the union of the preimages.
    */
  @pure
  @opaque
  def concatPreimage[K, V](m1: Map[K, V], m2: Map[K, V], s: Set[V]): Unit = {
    if (!(m1 ++ m2).preimage(s).subsetOf(m1.preimage(s) ++ m2.preimage(s))) {
      val k: K =
        SetProperties.notSubsetOfWitness((m1 ++ m2).preimage(s), m1.preimage(s) ++ m2.preimage(s))
      MapAxioms.preimageGet(m1 ++ m2, s, k)
      MapAxioms.preimageGet(m1, s, k)
      MapAxioms.preimageGet(m2, s, k)
      SetProperties.unionContains(m1.preimage(s), m2.preimage(s), k)
      concatContains(m1, m2, k)
      concatGet(m1, m2, k)
    }
  }.ensuring((m1 ++ m2).preimage(s).subsetOf(m1.preimage(s) ++ m2.preimage(s)))

  /** The preimage of a concatenation between two maps is a subset of the union of the preimages.
    */
  @pure
  @opaque
  def concatPreimage[K, V](m1: Map[K, V], m2: Map[K, V], s: V): Unit = {
    concatPreimage(m1, m2, Set(s))
    unfold(m1.preimage(s))
    unfold(m2.preimage(s))
    unfold((m1 ++ m2).preimage(s))
  }.ensuring((m1 ++ m2).preimage(s).subsetOf(m1.preimage(s) ++ m2.preimage(s)))

  /** The preimageof a set wrt to a map to which a pair has been added is a subset of the preimage of the original map
    * to which the mapping of the pair is added.
    */
  @pure
  @opaque
  def inclPreimage[K, V](m: Map[K, V], k: K, v: V, s: Set[V]): Unit = {
    val singl: Map[K, V] = Map(k -> v)
    unfold(m.updated(k, v))
    unfold(m.preimage(s).incl(k))
    concatPreimage(m, singl, s)
    preimageSingleton(k, v, s)
    if (s.contains(v)) {
      SetProperties.unionEqualsRight(m.preimage(s), singl.preimage(s), Set(k))
      SetProperties.subsetOfEqualsTransitivity(
        m.updated(k, v).preimage(s),
        m.preimage(s) ++ singl.preimage(s),
        m.preimage(s) + k,
      )
    } else {
      SetProperties.unionEqualsRight(m.preimage(s), singl.preimage(s), Set.empty)
      SetProperties.unionEmpty(m.preimage(s))
      SetProperties.equalsTransitivity(
        m.preimage(s) ++ singl.preimage(s),
        m.preimage(s) ++ Set.empty,
        m.preimage(s),
      )
      SetProperties.subsetOfEqualsTransitivity(
        m.updated(k, v).preimage(s),
        m.preimage(s) ++ singl.preimage(s),
        m.preimage(s),
      )
      SetProperties.subsetOfTransitivity(
        m.updated(k, v).preimage(s),
        m.preimage(s),
        m.preimage(s) + k,
      )
    }
  }.ensuring(
    if (s.contains(v))
      m.updated(k, v).preimage(s).subsetOf(m.preimage(s) + k)
    else
      m.updated(k, v).preimage(s).subsetOf(m.preimage(s)) && m
        .updated(k, v)
        .preimage(s)
        .subsetOf(m.preimage(s) + k)
  )

  /** The preimageof a set wrt to a map to which a pair has been added is a subset of the preimage of the original map
    * to which the mapping of the pair is added.
    */
  @pure
  @opaque
  def inclPreimage[K, V](m: Map[K, V], k: K, v: V, s: V): Unit = {
    unfold(m.updated(k, v).preimage(s))
    unfold(m.preimage(s))
    inclPreimage(m, k, v, Set(s))
    SetProperties.singletonContains(s, v)
  }.ensuring(
    if (s == v)
      m.updated(k, v).preimage(s).subsetOf(m.preimage(s) + k)
    else
      m.updated(k, v).preimage(s).subsetOf(m.preimage(s)) && m
        .updated(k, v)
        .preimage(s)
        .subsetOf(m.preimage(s) + k)
  )

  /** --------------------------------------------------------------------------------------------------------------- *
    * ------------------------------------------------FIND------------------------------------------------------------ *
    * ----------------------------------------------------------------------------------------------------------------
    */

  /** If a map is empty, then find will always return None
    */
  @pure
  @opaque
  def findEmpty[K, V](m: Map[K, V], f: ((K, V)) => Boolean): Unit = {
    require(m === Map.empty[K, V])
    if (m.find(f).isDefined) {
      findGet(m, f)
      unfold(m.contains)
      equalsContains(m, Map.empty[K, V], m.find(f).get._1)
      emptyContains[K, V](m.find(f).get._1)
    }
  }.ensuring(!m.find(f).isDefined)

  @pure
  @opaque
  def findEquals[K, V](m1: Map[K, V], m2: Map[K, V], f: ((K, V)) => Boolean): Unit = {
    require(m1 === m2)
    if (m1.find(f).isDefined) {
      findGet(m1, f)
      equalsGet(m1, m2, m1.find(f).get._1)
      findDefined(m2, f, m1.find(f).get._1, m1.find(f).get._2)
    } else if (m2.find(f).isDefined) {
      findGet(m2, f)
      equalsGet(m2, m1, m2.find(f).get._1)
      findDefined(m1, f, m2.find(f).get._1, m2.find(f).get._2)
    }
  }.ensuring(m1.find(f).isDefined == m2.find(f).isDefined)

}

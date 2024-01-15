// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package lf.verified
package utils

import stainless.lang.{unfold, Option, None, Some, BooleanDecorations}
import stainless.annotation._
import scala.annotation.targetName
import scala.collection.{Map => ScalaMap}

import MapProperties._
import SetProperties._

case class Map[K, V](@pure @extern toScala: ScalaMap[K, V]) {

  import MapAxioms._

  @pure @extern
  def get(k: K): Option[V] = ???

  @pure @opaque
  def getOrElse(k: K, d: V): V = get(k).getOrElse(d)

  @pure
  def view: Map[K, V] = this

  @pure
  def toMap: Map[K, V] = this

  @pure @extern
  def preimage(s: Set[V]): Set[K] = ???

  @pure
  @extern
  def find(f: ((K, V)) => Boolean): Option[(K, V)] = ???
  @pure
  @extern
  def foldLeft[B](z: B)(op: (B, (K, V)) => B): B = ???
  @pure @opaque
  def preimage(v: V): Set[K] = preimage(Set(v))

  @pure @extern
  def submapOf(m2: Map[K, V]): Boolean = ???

  @pure @opaque
  def apply(k: K): V = {
    require(contains(k))
    unfold(contains)
    get(k).get
  }.ensuring(Some[V](_) == get(k))

  @pure @opaque
  def contains: K => Boolean = get(_).isDefined

  @pure @extern
  def concat(m2: Map[K, V]): Map[K, V] = Map(toScala ++ m2.toScala)
  @pure @alias
  def ++(s2: Map[K, V]): Map[K, V] = concat(s2)

  @pure @opaque
  def updated(k: K, v: V): Map[K, V] = {
    val res = concat(Map(k -> v))

    def updatedProperties: Unit = {
      unfold(res.contains)
      singletonGet(k, v, k)
      concatGet(this, Map(k -> v), k)

      // values
      singletonValues(k, v)
      concatValues(this, Map(k -> v))
      unfold(values.incl(v))
      unionEqualsRight(values, Map(k -> v).values, Set(v))
      SetProperties.subsetOfEqualsTransitivity(
        res.values,
        values ++ Map(k -> v).values,
        values ++ Set(v),
      )

      singletonKeySet(k, v)
      concatKeySet(this, Map(k -> v))
      unionEqualsRight(keySet, Map(k -> v).keySet, Set(k))
      SetProperties.equalsTransitivity(res.keySet, keySet ++ Map(k -> v).keySet, keySet ++ Set(k))
      unfold(keySet.incl(k))
      if (!submapOf(res) && (!contains(k) || (get(k) == Some[V](v)))) {
        val w = notSubmapOfWitness(this, res)
        concatGet(this, Map(k -> v), w)
        singletonGet(k, v, w)
      }
      if (submapOf(res) && contains(k) && (get(k) != Some[V](v))) {
        submapOfGet(this, res, k)
      }
    }.ensuring(
      res.contains(k) &&
        res.get(k) == Some[V](v) &&
        res.keySet === keySet + k &&
        res.values.subsetOf(values + v) &&
        (submapOf(res) == (!contains(k) || (get(k) == Some[V](v))))
    )

    updatedProperties
    res
  }.ensuring(res =>
    res.contains(k) &&
      res.get(k) == Some[V](v) &&
      res.keySet === keySet + k &&
      res.values.subsetOf(values + v) &&
      (submapOf(res) == (!contains(k) || get(k) == Some[V](v)))
  )

  @pure @opaque
  def keySet: Set[K] = preimage(values)

  @pure @extern
  def values: Set[V] = ???

  @pure @targetName("mapValuesArgSingle")
  def mapValues[V2](f: V => V2): Map[K, V2] = {
    mapValues((k: K) => f)
  }

  @pure @opaque @targetName("mapValuesArgPair")
  def mapValues[V2](f: K => V => V2): Map[K, V2] = {
    map { case (k, v) => k -> f(k)(v) }
  }

  @pure
  @extern
  def map[K2, V2](f: ((K, V)) => (K2, V2)): Map[K2, V2] = Map(toScala.map(f))

  @pure @extern
  def filter(f: ((K, V)) => Boolean): Map[K, V] = Map(toScala.filter(f))

  @pure
  def ===(m2: Map[K, V]): Boolean = {
    submapOf(m2) && m2.submapOf(this)
  }

  @pure
  def =/=(m2: Map[K, V]): Boolean = !(this === m2)

}

object Map {

  @pure @extern
  def empty[K, V]: Map[K, V] = Map[K, V](ScalaMap[K, V]())

  @pure @extern
  def apply[K, V](p: (K, V)): Map[K, V] = Map[K, V](ScalaMap[K, V](p))

}

object MapAxioms {

  /** Getting from a concatenation is equivalent to getting in the second map and in case of failure in the first one.
    */
  @pure @extern
  def concatGet[K, V](m1: Map[K, V], m2: Map[K, V], ks: K): Unit = {}.ensuring(
    (m1 ++ m2).get(ks) == m2.get(ks).orElse(m1.get(ks))
  )

  /** Getting in an empty map will always result in a failure
    */
  @pure
  @extern
  def emptyGet[K, V](ks: K): Unit = {}.ensuring(Map.empty[K, V].get(ks) == None[V]())

  /** Getting from a singleton will be defined if and only if the query is the key of the pair. In this case the returned
    * mapping is the value of the pair.
    */
  @pure @extern
  def singletonGet[K, V](k: K, v: V, ks: K): Unit = {}.ensuring(
    Map(k -> v).get(ks) == (if (k == ks) Some(v) else None[V]())
  )

  /** Getting in a map after having mapped its value is the same as getting in the original map and afterward applying
    * the function on the returned mapping
    */
  @pure @extern
  def mapValuesGet[K, V, V2](m: Map[K, V], f: K => V => V2, k: K): Unit = {}.ensuring(
    m.mapValues(f).get(k) == m.get(k).map(f(k))
  )

  /** If a map is a submap of another one and it contains a given key, then that key is bound to the same mapping in both
    * maps
    */
  @pure @extern
  def submapOfGet[K, V](m1: Map[K, V], m2: Map[K, V], k: K): Unit = {
    require(m1.submapOf(m2))
    require(m1.contains(k))
  }.ensuring(m1.get(k) == m2.get(k))

  /** If a map is not submap of another one then we can exhibit a key in the first map such that their mapping is different
    */
  @pure
  @extern
  def notSubmapOfWitness[K, V](m1: Map[K, V], m2: Map[K, V]): K = {
    require(!m1.submapOf(m2))
    ??? : K
  }.ensuring(res => m1.contains(res) && (m1.get(res) != m2.get(res)))

  @pure
  @extern
  def preimageGet[K, V](m: Map[K, V], s: Set[V], k: K): Unit = {}.ensuring(
    m.preimage(s).contains(k) == (m.get(k).exists(s.contains))
  )

  /** If the values of a map contain a value then we can exhibit a key such that its mapping in the map is equal to the
    * value
    */
  @pure @extern
  def valuesWitness[K, V](m: Map[K, V], v: V): K = {
    require(m.values.contains(v))
    (??? : K)
  }.ensuring(res => m.get(res) == Some[V](v))

  /** If map contains a key then the values of the map contain its mapping
    */
  @pure
  @extern
  def valuesContains[K, V](m: Map[K, V], k: K): Unit = {
    require(m.contains(k))
  }.ensuring(m.values.contains(m(k)))

  /** If find is defined then
    */
  @pure
  @extern
  def findGet[K, V](m: Map[K, V], f: ((K, V)) => Boolean): Unit = {
    require(m.find(f).isDefined)
  }.ensuring(
    m.get(m.find(f).get._1) == Some[V](m.find(f).get._2) && f((m.find(f).get))
  )

  /** If if there is a value in the map that satisfies a given predicate then find is defined.
    */
  @pure
  @extern
  def findDefined[K, V](m: Map[K, V], f: ((K, V)) => Boolean, k: K, v: V): Unit = {
    require(m.get(k) == Some[V](v))
    require(f(k, v))
  }.ensuring(
    m.find(f).isDefined
  )

  /** Extensionality axiom
    *
    * If two maps are submap of each other then their are equal
    */
  @pure @extern
  def extensionality[K, V](m1: Map[K, V], m2: Map[K, V]): Unit = {
    require(m1 === m2)
  }.ensuring(m1 == m2)

}

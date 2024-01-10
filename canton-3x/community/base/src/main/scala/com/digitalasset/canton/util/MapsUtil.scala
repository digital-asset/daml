// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.FlatMap
import cats.data.Chain
import cats.kernel.Semigroup
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.logging.ErrorLoggingContext

import scala.annotation.tailrec
import scala.collection.{concurrent, mutable}

object MapsUtil {

  /** Merges two maps where the values are sets of V
    *
    * @param big the likely larger of the two maps (so we can merge the small into the big,
    *            rather than the other way around)
    * @param small the likely smaller of the maps.
    */
  def mergeMapsOfSets[K, V](big: Map[K, Set[V]], small: Map[K, Set[V]]): Map[K, Set[V]] = {
    small.foldLeft(big) { case (acc, (k, v)) =>
      acc.updated(k, acc.getOrElse(k, Set()).union(v))
    }
  }

  /** Atomically modifies the given map at the given key.
    * `notFound` may be evaluated even if the key is present at the atomic update;
    * in this case, its monadic effect propagates to the result.
    * `f` may be evaluated several times on several of the previous values associated to the key,
    * even if no value is associated with the key immediately before the atomic update happens.
    * The monadic effect of all these evaluations propagates to the result.
    *
    * @param map      The map that is updated
    * @param notFound The value to be used if the key was not present in the map.
    *                 [[scala.None$]] denotes that the map should not be modified.
    * @param f        The function used to transform the value found in the map.
    *                 If the returned value is [[scala.None$]], the key is removed from the map.
    * @return The value associated with the key before the update.
    */
  def modifyWithConcurrentlyM[F[_], K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => F[Option[V]],
      f: V => F[Option[V]],
  )(implicit monad: FlatMap[F]): F[Option[V]] = {
    // Make sure that we evaluate `notFound` at most once
    lazy val notFoundV = notFound

    def step(): F[Either[Unit, Option[V]]] = map.get(key) match {
      case None =>
        monad.map(notFoundV) {
          case None => Either.right[Unit, Option[V]](None)
          case Some(newValue) =>
            map
              .putIfAbsent(key, newValue)
              .fold(Either.right[Unit, Option[V]](None))(_ => Either.left[Unit, Option[V]](()))
        }
      case Some(oldValue) =>
        monad.map(f(oldValue)) {
          case None => Either.cond(map.remove(key, oldValue), Some(oldValue), ())
          case Some(newValue) =>
            Either.cond(map.replace(key, oldValue, newValue), Some(oldValue), ())
        }
    }

    monad.tailRecM(())((_: Unit) => step())
  }

  /** Atomically updates the given map at the given key. In comparison to [[modifyWithConcurrentlyM]],
    * this method supports only inserting and updating elements. It does not support removing elements or preventing the
    * insertion of elements when they are not originally in the map.
    */
  def updateWithConcurrentlyM[F[_], K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => F[V],
      f: V => F[V],
  )(implicit monad: FlatMap[F]): F[Option[V]] =
    modifyWithConcurrentlyM(
      map,
      key,
      monad.map(notFound)(Some(_)),
      (v: V) => monad.map(f(v))(Some(_)),
    )

  def modifyWithConcurrentlyM_[F[_], K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => F[Option[V]],
      f: V => F[Option[V]],
  )(implicit monad: FlatMap[F]): F[Unit] =
    monad.void(modifyWithConcurrentlyM(map, key, notFound, f))

  def updateWithConcurrentlyM_[F[_], K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => F[V],
      f: V => F[V],
  )(implicit monad: FlatMap[F]): F[Unit] =
    monad.void(updateWithConcurrentlyM(map, key, notFound, f))

  /** Specializes [[updateWithConcurrentlyM_]] to the [[Checked]] monad,
    * where the non-aborts are only kept from the invocation of `notFound` or `f`
    * that causes the first [[Checked.Abort]] or updates the map.
    */
  def modifyWithConcurrentlyChecked_[A, N, K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => Checked[A, N, Option[V]],
      f: V => Checked[A, N, Option[V]],
  ): Checked[A, N, Unit] = {
    /* The update function `f` may execute several times for different values,
     * In that case, we may get several non-aborts reported
     * for the same contract update.
     *
     * To keep only those from the last update, we group the problems in `Chain`s and only keep the last one.
     */
    def lift(chain: Chain[N]): Chain[Chain[N]] = Chain(chain)

    MapsUtil
      .modifyWithConcurrentlyM_(
        map,
        key,
        notFound.mapNonaborts(lift),
        (x: V) => f(x).mapNonaborts(lift),
      )
      .mapNonaborts { chainsOfNonaborts =>
        ChainUtil.lastOption(chainsOfNonaborts).getOrElse(Chain.empty)
      }
  }

  def updateWithConcurrentlyChecked_[A, N, K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => Checked[A, N, V],
      f: V => Checked[A, N, V],
  ): Checked[A, N, Unit] =
    modifyWithConcurrentlyChecked_(map, key, notFound.map(Some(_)), (v: V) => f(v).map(Some.apply))

  /** @param m An input map
    * @param f A function to map each of the input keys to a new set of keys ks
    *
    * Generates a new map m' with the following property:
    * If { (k,v) in m and k' in f(k) } then v is in the set m'[k']
    *
    * See `MapsUtilTest` for an example.
    */
  def groupByMultipleM[M[_], K, K2, V](
      m: Map[K, V]
  )(f: K => M[Set[K2]])(implicit M: cats.Monad[M]): M[Map[K2, Set[V]]] = {
    m.toList.foldM(Map.empty[K2, Set[V]]) { case (m, (k, v)) =>
      M.map(f(k)) {
        _.toList.foldLeft(m) { (m_, k2) =>
          val newVal = m_.getOrElse(k2, Set.empty[V]) + v
          m_ + (k2 -> newVal)
        }
      }
    }
  }

  /** Updates the key of the current map if present.
    * The update function `f` may be evaluated multiple times.
    * if `f` throws an exception, the exception propagates and the map is not updated.
    *
    * @return Whether the map changed due to this update
    */
  def updateWithConcurrently[K, V <: AnyRef](map: concurrent.Map[K, V], key: K)(
      f: V => V
  ): Boolean = {
    @tailrec def go(): Boolean = map.get(key) match {
      case None => false
      case Some(current) =>
        val next = f(current)
        if (current eq next)
          false // Do not modify the map if the transformation doesn't change anything
        else if (map.replace(key, current, next)) true
        else go() // concurrent modification, so let's retry
    }
    go()
  }

  /** Insert the pair (key, value) to `map` if not already present.
    * Assert that any existing element for `key` is equal to `value`.
    * @throws java.lang.IllegalStateException if the assertion fails
    */
  def tryPutIdempotent[K, V](map: concurrent.Map[K, V], key: K, value: V)(implicit
      loggingContext: ErrorLoggingContext
  ): Unit =
    map.putIfAbsent(key, value).foreach { oldValue =>
      ErrorUtil.requireState(
        oldValue == value,
        s"Map key $key already has value $oldValue assigned to it. Cannot insert $value.",
      )
    }

  def mergeWith[K, V](map1: Map[K, V], map2: Map[K, V])(f: (V, V) => V): Map[K, V] = {
    // We don't need `f`'s associativity when we merge maps
    implicit val semigroupV = new Semigroup[V] {
      override def combine(x: V, y: V): V = f(x, y)
    }
    Semigroup[Map[K, V]].combine(map1, map2)
  }

  def extendMapWith[K, V](m: mutable.Map[K, V], extendWith: IterableOnce[(K, V)])(
      merge: (V, V) => V
  ): Unit = {
    extendWith.iterator.foreach { case (k, v) =>
      m.updateWith(k) {
        case None => Some(v)
        case Some(mv) => Some(merge(mv, v))
      }.discard[Option[V]]
    }
  }

  def extendedMapWith[K, V](m: Map[K, V], extendWith: IterableOnce[(K, V)])(
      merge: (V, V) => V
  ): Map[K, V] =
    extendWith.iterator.foldLeft(m) { case (acc, (k, v)) =>
      acc.updatedWith(k) {
        case None => Some(v)
        case Some(mv) => Some(merge(mv, v))
      }
    }

  /** Return all key-value pairs in minuend that are different / missing in subtrahend
    *
    * @throws java.lang.IllegalArgumentException if the minuend is not defined for all keys of the subtrahend.
    */
  def mapDiff[K, V](minuend: Map[K, V], subtrahend: collection.Map[K, V])(implicit
      loggingContext: ErrorLoggingContext
  ): Map[K, V] = {
    ErrorUtil.requireArgument(
      subtrahend.keySet.subsetOf(subtrahend.keySet),
      s"Cannot compute map difference if minuend is not defined whenever subtrahend is defined. Missing keys: ${subtrahend.keySet diff minuend.keySet}",
    )
    minuend.filter { case (k, v) => !subtrahend.get(k).contains(v) }
  }
}

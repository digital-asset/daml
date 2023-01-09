// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.types

import scalaz.{Applicative, Comonad, Order, Traverse, ==>>}
import scalaz.std.tuple._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.traverse._

/** Cofree[K ==>> *, A], except gremlins have infested scalaz with
  * trampolines
  */
final case class Namespace[K, A](here: A, subtree: K ==>> Namespace[K, A]) {

  /** Like scanr but just return the root result. */
  def foldTreeStrict[B](f: (A, K ==>> B) => B): B =
    f(here, subtree map (_ foldTreeStrict f))

  def scanr[B](f: (A, K ==>> Namespace[K, B]) => B): Namespace[K, B] = {
    val newSubtree = subtree map (_ scanr f)
    Namespace(f(here, newSubtree), newSubtree)
  }
}

object Namespace {
  // could be Traverse1 instead of Traverse but I don't know we'll need it
  implicit def `Namespace covariant`[K]: Comonad[Namespace[K, *]] with Traverse[Namespace[K, *]] =
    new Comonad[Namespace[K, *]] with Traverse[Namespace[K, *]] {
      override def cobind[A, B](fa: Namespace[K, A])(f: Namespace[K, A] => B): Namespace[K, B] =
        Namespace(f(fa), fa.subtree.map(cobind(_)(f)))

      override def copoint[A](p: Namespace[K, A]): A = p.here

      override def map[A, B](fa: Namespace[K, A])(f: A => B): Namespace[K, B] =
        Namespace(f(fa.here), fa.subtree.map(map(_)(f)))

      // preorder fold/traversal (there is no inherent notion of an
      // ordering containing both the root and child elements)
      override def traverseImpl[G[_]: Applicative, A, B](fa: Namespace[K, A])(
          f: A => G[B]
      ): G[Namespace[K, B]] =
        ^(f(fa.here), fa.subtree traverse (traverseImpl(_)(f)))(Namespace(_, _))
    }

  /** Build a tree from name elements K; the root element is the empty
    * name.  Invariant: no duplicate List[K]s.
    */
  def fromHierarchy[K: Order, V](elts: Iterable[(List[K], V)]): Namespace[K, Option[V]] = {
    val (subs, here) = elts partition (_._1.nonEmpty)
    Namespace(
      here.headOption map (_._2),
      ==>>(subs.groupBy(_._1.head).toSeq: _*)
        .map(children => fromHierarchy(children.map(_.leftMap(_.tail)))),
    )
  }
}

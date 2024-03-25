// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.Nested
import cats.{Applicative, Eval, Id, Traverse}
import org.apache.pekko.stream.scaladsl.Keep

/** [[cats.Traverse]] for containers with at most one element.
  */
trait SingletonTraverse[F[_]] extends Traverse[F] { self =>

  /** The type for actual context of the element.
    * For example, for `F[A] = Either[String, (Int, A)]`, the `Context` should be `Int`.
    * This gives better type information than considering the general context type `Either[String, *]`
    * because `Either[String, *]` could also be a [[scala.Left$]] and thus not contain an `Int` at all.
    */
  type Context

  /** Threads the effect `G` of the given function `f` through this container.
    * Also passes the context of this container (i.e., the container structure) to the function.
    */
  def traverseSingleton[G[_], A, B](x: F[A])(f: (Context, A) => G[B])(implicit
      G: Applicative[G]
  ): G[F[B]]

  /** Composes this type class instance with another type class instance.
    *
    * Apply the result to a combination function for the contexts of the two type class instances.
    * This allows to throw away the context of one instance, e.g., if it does not contain
    * any information like in the case of `Option` or `Either`.
    * The default [[org.apache.pekko.stream.scaladsl.Keep.both]] retains both contexts.
    */
  // Partial application to help with type inference:
  // The parameter `G` defines one parameter type of the combination function.
  def composeWith[G[_]](
      G: SingletonTraverse[G]
  ): SingletonTraverse.ComposeSingletonTraversePartiallyApplied[F, G, Context, G.Context] =
    new SingletonTraverse.ComposeSingletonTraversePartiallyApplied[F, G, Context, G.Context](
      this,
      G,
    )
}

object SingletonTraverse {

  /** Make the dependent Context type explicit as a type argument to help with type inference.
    *
    * The construction is the same as in shapeless.
    * [The Type Astronaut's Guide to Shapeless Book](https://underscore.io/books/shapeless-guide/)
    * explains the idea in Chapter 4.2.
    */
  type Aux[F[_], C] = SingletonTraverse[F] { type Context = C }

  def apply[F[_]](implicit F: SingletonTraverse[F]): SingletonTraverse.Aux[F, F.Context] = F

  def composeWith[F[_], G[_], C](FF: SingletonTraverse[F], GG: SingletonTraverse[G])(
      combine: (FF.Context, GG.Context) => C
  ): SingletonTraverse.Aux[Lambda[a => F[G[a]]], C] = new ComposedSingletonTraverse[F, G, C] {
    override val F: SingletonTraverse.Aux[F, FF.Context] = FF
    override val G: SingletonTraverse.Aux[G, GG.Context] = GG

    override def combineContext(fc: FF.Context, gc: GG.Context): C = combine(fc, gc)
  }

  private[util] class ComposeSingletonTraversePartiallyApplied[F[_], G[_], FC, GC](
      F: SingletonTraverse.Aux[F, FC],
      G: SingletonTraverse.Aux[G, GC],
  ) {
    def apply[C](
        combine: (FC, GC) => C = Keep.both
    ): SingletonTraverse.Aux[Lambda[a => F[G[a]]], C] =
      composeWith(F, G)(combine)
  }

  /** Method implementations for [[cats.Traverse]] and ancestors are taken from [[cats.ComposedTraverse]]
    * and ancestors as they are package-private to cats. :-(
    */
  private trait ComposedSingletonTraverse[F[_], G[_], C]
      extends SingletonTraverse[Lambda[a => F[G[a]]]] {
    override type Context = C

    val F: SingletonTraverse[F]
    val G: SingletonTraverse[G]
    def combineContext(fc: F.Context, gc: G.Context): Context

    override def traverseSingleton[H[_], A, B](fga: F[G[A]])(f: (Context, A) => H[B])(implicit
        H: Applicative[H]
    ): H[F[G[B]]] = {
      F.traverseSingleton(fga) { (contextF, ga) =>
        G.traverseSingleton(ga) { (contextG, x) => f(combineContext(contextF, contextG), x) }
      }
    }

    override def map[A, B](fga: F[G[A]])(f: A => B): F[G[B]] =
      F.map(fga)(ga => G.map(ga)(f))

    override def foldLeft[A, B](fga: F[G[A]], b: B)(f: (B, A) => B): B =
      F.foldLeft(fga, b)((b, ga) => G.foldLeft(ga, b)(f))

    override def foldRight[A, B](fga: F[G[A]], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      F.foldRight(fga, lb)((ga, lb) => G.foldRight(ga, lb)(f))

    override def toList[A](fga: F[G[A]]): List[A] =
      F.toList(fga).flatMap(G.toList)

    override def toIterable[A](fga: F[G[A]]): Iterable[A] =
      F.toIterable(fga).flatMap(G.toIterable)

    override def traverse[H[_]: Applicative, A, B](fga: F[G[A]])(f: A => H[B]): H[F[G[B]]] =
      F.traverse(fga)(ga => G.traverse(ga)(f))

    override def mapAccumulate[S, A, B](init: S, fga: F[G[A]])(f: (S, A) => (S, B)): (S, F[G[B]]) =
      F.mapAccumulate(init, fga)((s, ga) => G.mapAccumulate(s, ga)(f))
  }

  // Instances

  abstract class FromTraverse[F[_]](delegate: Traverse[F]) extends SingletonTraverse[F] {
    override def traverse[G[_], A, B](fa: F[A])(f: A => G[B])(implicit G: Applicative[G]): G[F[B]] =
      delegate.traverse(fa)(f)

    override def foldLeft[A, B](fa: F[A], b: B)(f: (B, A) => B): B =
      delegate.foldLeft(fa, b)(f)

    override def foldRight[A, B](fa: F[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      delegate.foldRight(fa, lb)(f)
  }

  implicit val singletonTraverseCatsId: SingletonTraverse.Aux[Id, Unit] =
    new FromTraverse[Id](cats.Invariant.catsInstancesForId) {
      override type Context = Unit
      override def traverseSingleton[G[_], A, B](x: Id[A])(f: (Unit, A) => G[B])(implicit
          G: Applicative[G]
      ): G[Id[B]] = f((), x)
    }

  implicit def singletonTraverseCatsTuple2[A]: SingletonTraverse.Aux[(A, *), A] =
    new FromTraverse[(A, *)](cats.UnorderedFoldable.catsUnorderedFoldableInstancesForTuple2) {
      override type Context = A
      override def traverseSingleton[G[_], B, C](
          x: (A, B)
      )(f: (A, B) => G[C])(implicit G: Applicative[G]): G[(A, C)] = {
        val (a, _) = x
        traverse(x)(f(a, _))
      }
    }

  implicit val singletonTraverseOption: SingletonTraverse.Aux[Option, Unit] =
    new FromTraverse[Option](cats.UnorderedFoldable.catsTraverseForOption) {
      override type Context = Unit
      override def traverseSingleton[G[_], A, B](x: Option[A])(f: (Unit, A) => G[B])(implicit
          G: Applicative[G]
      ): G[Option[B]] =
        traverse(x)(f((), _))
    }

  implicit def singletonTraverseEither[A]: SingletonTraverse.Aux[Either[A, *], Unit] =
    new FromTraverse[Either[A, *]](cats.UnorderedFoldable.catsTraverseForEither) {
      override type Context = Unit
      override def traverseSingleton[G[_], B, C](x: Either[A, B])(f: (Unit, B) => G[C])(implicit
          G: Applicative[G]
      ): G[Either[A, C]] = traverse(x)(f((), _))
    }

  implicit def singletonTraverseNested[F[_], G[_], FC, GC](implicit
      F: SingletonTraverse.Aux[F, FC],
      G: SingletonTraverse.Aux[G, GC],
  ): SingletonTraverse.Aux[Nested[F, G, *], (FC, GC)] =
    new FromTraverse[Nested[F, G, *]](cats.data.Nested.catsDataTraverseForNested) {
      override type Context = (FC, GC)

      override def traverseSingleton[H[_], A, B](fga: Nested[F, G, A])(f: ((FC, GC), A) => H[B])(
          implicit H: Applicative[H]
      ): H[Nested[F, G, B]] =
        H.map(F.traverseSingleton(fga.value) { (fc, ga) =>
          G.traverseSingleton(ga)((gc, x) => f((fc, gc), x))
        })(Nested.apply)
    }

  trait Ops[F[_], C, A] extends Serializable {
    protected def self: F[A]
    protected val typeClassInstance: SingletonTraverse.Aux[F, C]
    def traverseSingleton[G[_], B](f: (C, A) => G[B])(implicit G: Applicative[G]): G[F[B]] =
      typeClassInstance.traverseSingleton(self)(f)
  }

  /** Extension method for instances of [[SingletonTraverse]]. */
  object syntax {
    import scala.language.implicitConversions
    implicit def SingletonTraverseOps[F[_], A](target: F[A])(implicit
        st: SingletonTraverse[F]
    ): Ops[F, st.Context, A] = new Ops[F, st.Context, A] {
      override val self: F[A] = target
      override protected val typeClassInstance: SingletonTraverse.Aux[F, st.Context] = st
    }
  }
}

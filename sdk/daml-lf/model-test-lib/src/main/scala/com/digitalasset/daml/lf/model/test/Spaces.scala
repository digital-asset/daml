// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import cats.Applicative
import scala.collection.mutable
import scala.util.control.TailCalls._

object Spaces {

  sealed trait Finite[A] {
    def *[B](that: Finite[B]): Finite[(A, B)] = FTimes(this, that)
    def +(that: Finite[A]): Finite[A] = FPlus(this, that)
    def map[B](f: A => B): Finite[B] = FMap(f, this)
    def ap[B](ff: Finite[A => B]): Finite[B] = (ff * this).map { case (f, a) => f(a) }

    private var cardinal_cache: BigInt = _
    private def computeCardinal() =
      if (cardinal_cache != null) done(cardinal_cache)
      else
        for {
          res <- Finite.cardinal(this)
          _ = cardinal_cache = res
        } yield res

    lazy val cardinal = computeCardinal().result

    def apply(i: BigInt): A = Finite.index(this, i).result
  }

  private final case class FEmpty[A]() extends Finite[A]
  private final case class FSingleton[A](a: A) extends Finite[A]
  private final case class FTimes[A, B](s1: Finite[A], s2: Finite[B]) extends Finite[(A, B)]
  private final case class FPlus[A](s1: Finite[A], s2: Finite[A]) extends Finite[A]
  private final case class FMap[A, B](f: A => B, s: Finite[A]) extends Finite[B]

  object Finite {
    def empty[A]: Finite[A] = FEmpty()
    def singleton[A](a: A): Finite[A] = FSingleton(a)

    def cardinal[A](fin: Finite[A]): TailRec[BigInt] = fin match {
      case FEmpty() => done(0)
      case FSingleton(_) => done(1)
      case FTimes(s1, s2) =>
        for {
          c1 <- tailcall(s1.computeCardinal())
          c2 <- tailcall(s2.computeCardinal())
        } yield c1 * c2
      case FPlus(s1, s2) =>
        for {
          c1 <- tailcall(s1.computeCardinal())
          c2 <- tailcall(s2.computeCardinal())
        } yield c1 + c2
      case FMap(_, s) => s.computeCardinal()
    }

    def index[A](fin: Finite[A], i: BigInt): TailRec[A] = fin match {
      case _ if i >= fin.cardinal =>
        throw new IndexOutOfBoundsException(i.toString)
      case FSingleton(a) =>
        // i is necessarily 0 thanks to the guard above
        done(a)
      case FTimes(s1, s2) =>
        val (q, r) = i /% s2.cardinal
        for {
          left <- tailcall(index(s1, q))
          right <- tailcall(index(s2, r))
        } yield (left, right)
      case FPlus(s1, s2) =>
        if (i < s1.cardinal) index(s1, i)
        else index(s2, i - s1.cardinal)
      case FMap(f, s) =>
        index(s, i).map(f)
      case _ =>
        throw new IllegalStateException(s"unexpected constructor $fin")
    }

    object Instances {
      implicit val applicativeFinite: Applicative[Finite] = new Applicative[Finite] {
        override def pure[A](a: A): Finite[A] = Finite.singleton(a)
        override def ap[A, B](ff: Finite[A => B])(fa: Finite[A]): Finite[B] = fa.ap(ff)
      }
    }
  }

  sealed trait Space[A] {
    def *[B](that: Space[B]): Space[(A, B)] = STimes(this, that)
    def +(that: Space[A]): Space[A] = SPlus(this, that)
    def map[B](f: A => B): Space[B] = SMap(f, this)
    def ap[B](ff: Space[A => B]): Space[B] = (ff * this).map { case (f, a) => f(a) }

    private val cache: mutable.Map[Int, Finite[A]] = new mutable.HashMap

    private def sized(n: Int): TailRec[Finite[A]] = cache.get(n) match {
      case Some(value) => done(value)
      case None =>
        for {
          res <- Space.computeSized(this, n)
          _ = cache(n) = res
        } yield res
    }

    def apply(n: Int): Finite[A] = cache.getOrElseUpdate(n, sized(n).result)
  }

  private final case class SEmpty[A]() extends Space[A]
  private final case class SSingleton[A](a: A) extends Space[A]
  private final case class STimes[A, B](s1: Space[A], s2: Space[B]) extends Space[(A, B)]
  private final case class SPlus[A](s1: Space[A], s2: Space[A]) extends Space[A]
  private final case class SMap[A, B](f: A => B, s: Space[A]) extends Space[B]
  private final case class SPay[A](spaceProducer: () => Space[A]) extends Space[A] {
    lazy val space = spaceProducer()
  }

  object Space {
    import cats.instances.all._
    import cats.syntax.all._

    def empty[A]: Space[A] = SEmpty()
    def singleton[A](a: A): Space[A] = SSingleton(a)
    def pay[A](s: => Space[A]): Space[A] = new SPay(() => s)

    def computeSized[A](sp: Space[A], n: Int): TailRec[Finite[A]] = sp match {
      case SEmpty() => done(Finite.empty)
      case SSingleton(a) =>
        if (n == 0) done(Finite.singleton(a))
        else done(Finite.empty)
      case STimes(s1, s2) =>
        for {
          products <- (0 to n).toList.traverse((i: Int) =>
            for {
              fin1 <- tailcall(s1.sized(i))
              fin2 <- tailcall(s2.sized(n - i))
            } yield fin1 * fin2
          )
        } yield products.fold(Finite.empty)(_ + _)
      case SPlus(s1, s2) =>
        for {
          fin1 <- tailcall(s1.sized(n))
          fin2 <- tailcall(s2.sized(n))
        } yield fin1 + fin2
      case SMap(f, s) =>
        for {
          fin <- tailcall(s.sized(n))
        } yield fin.map(f)
      case pay: SPay[A] =>
        if (n > 0) tailcall(pay.space.sized(n - 1))
        else done(Finite.empty)
    }

    object Instances {
      implicit val applicativeFinite: Applicative[Space] = new Applicative[Space] {
        override def pure[A](a: A): Space[A] = Space.singleton(a)
        override def ap[A, B](ff: Space[A => B])(fa: Space[A]): Space[B] = fa.ap(ff)
      }
    }
  }
}

// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import scalaz.{-\/, Bifunctor, \/, \/-}
import scalaz.syntax.bifunctor._
import scalaz.std.tuple._
import akka.NotUsed
import akka.stream.{BidiShape, FanOutShape2, Graph}
import akka.stream.scaladsl.{Concat, Flow, GraphDSL, Partition}

import scala.annotation.tailrec

import com.daml.scalautil.Statement.discard

import scala.collection.compat._
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.{IndexedSeq, Iterable, LinearSeq}

/** A variant of [[scalaz.CorecursiveList]] that emits a final state
  * at the end of the list.
  */
private[trigger] sealed abstract class UnfoldState[+T, +A] {
  type S
  val init: S
  val step: S => T \/ (A, S)

  def withInit(init: S): UnfoldState.Aux[S, T, A]

  final def foreach(f: A => Unit): T = {
    @tailrec def go(s: S): T = step(s) match {
      case -\/(t) => t
      case \/-((a, s2)) =>
        f(a)
        go(s2)
    }
    go(init)
  }

  final def runTo[FA](implicit cbf: CanBuildFrom[Nothing, A, FA]): (FA, T) = {
    val b = cbf()
    val t = foreach(a => discard(b += a))
    (b.result(), t)
  }
}

private[trigger] object UnfoldState {
  type Aux[S0, +T, +A] = UnfoldState[T, A] { type S = S0 }

  def apply[S, T, A](init: S)(step: S => T \/ (A, S)): UnfoldState[T, A] = {
    type S0 = S
    final case class UnfoldStateImpl(init: S, step: S => T \/ (A, S)) extends UnfoldState[T, A] {
      type S = S0
      override def withInit(init: S) = copy(init = init)
    }
    UnfoldStateImpl(init, step)
  }

  implicit def `US bifunctor instance`: Bifunctor[UnfoldState] = new Bifunctor[UnfoldState] {
    override def bimap[A, B, C, D](fab: UnfoldState[A, B])(f: A => C, g: B => D) =
      UnfoldState(fab.init)(fab.step andThen (_ bimap (f, (_ leftMap g))))
  }

  def fromLinearSeq[A](list: LinearSeq[A]): UnfoldState[Unit, A] =
    apply(list) {
      case hd +: tl => \/-((hd, tl))
      case _ => -\/(())
    }

  def fromIndexedSeq[A](vector: IndexedSeq[A]): UnfoldState[Unit, A] =
    apply(0) { n =>
      if (vector.sizeIs > n) \/-((vector(n), n + 1))
      else -\/(())
    }

  /** A stateful but pure version of built-in flatMapConcat.
    * (flatMapMerge does not make sense, because parallelism
    * with linear state does not make sense.)
    */
  def flatMapConcat[T, A, B](zero: T)(f: (T, A) => UnfoldState[T, B]): Flow[A, B, NotUsed] =
    flatMapConcatStates(zero)(f) collect { case \/-(b) => b }

  /** Like `flatMapConcat` but emit the new state after each unfolded list.
    * The pattern you will see is a bunch of right Bs, followed by a single
    * left T, then repeat until close, with a final T unless aborted.
    */
  def flatMapConcatStates[T, A, B](zero: T)(
      f: (T, A) => UnfoldState[T, B]): Flow[A, T \/ B, NotUsed] =
    Flow[A].statefulMapConcat(() => mkMapConcatFun(zero, f))

  /** Like `flatMapConcat` but emit the new state after each unfolded list.
    * The pattern you will see is a bunch of right Bs, followed by a single
    * left T, then repeat until close, with a final T unless aborted.
    */
  def flatMapConcatNode[T, A, B](
      f: (T, A) => UnfoldState[T, B]): Graph[BidiShape[T, B, A, T], NotUsed] =
    GraphDSL.create() { implicit gb =>
      import GraphDSL.Implicits._
      val initialT = gb add (Flow fromFunction \/.left[T, A])
      val as = gb add (Flow fromFunction \/.right[T, A])
      val tas = gb add Concat[T \/ A](2) // ensure that T arrives *before* A
      val splat = gb add (Flow[T \/ A] statefulMapConcat (() => statefulMapConcatFun(f)))
      val split = gb add partition[T, B]
      // format: off
      discard { initialT ~> tas }
      discard {       as ~> tas ~> splat ~> split.in }
      // format: on
      new BidiShape(initialT.in, split.out1, as.in, split.out0)
    }

  // TODO factor with ContractsFetch
  private[this] def partition[A, B]: Graph[FanOutShape2[A \/ B, A, B], NotUsed] =
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val split = b.add(Partition[A \/ B](2, {
        case -\/(_) => 0
        case \/-(_) => 1
      }))
      val as = b.add(Flow[A \/ B].collect { case -\/(a) => a })
      val bs = b.add(Flow[A \/ B].collect { case \/-(b) => b })
      discard { split ~> as }
      discard { split ~> bs }
      new FanOutShape2(split.in, as.out, bs.out)
    }

  private[this] def statefulMapConcatFun[T, A, B](
      f: (T, A) => UnfoldState[T, B]): T \/ A => Iterable[T \/ B] = {
    var mcFun: A => Iterable[T \/ B] = null
    _ fold (zeroT => {
      mcFun = mkMapConcatFun(zeroT, f)
      Iterable.empty
    }, { a =>
      mcFun(a)
    })
  }

  private[this] def mkMapConcatFun[T, A, B](
      zero: T,
      f: (T, A) => UnfoldState[T, B]): A => Iterable[T \/ B] = {
    var t = zero
    // statefulMapConcat only uses 'iterator'.  We preserve the Iterable's
    // immutability by making one strict reference to the 't' var at 'Iterable' creation
    // time, meaning any later 'iterator' call uses the same start state, no matter
    // whether the 't' has been updated
    a =>
      new Iterable[T \/ B] {
        private[this] val bs = f(t, a)
        import bs.step
        override def iterator = new Iterator[T \/ B] {
          private[this] var last: Option[T \/ (B, bs.S)] = {
            val fst = step(bs.init)
            fst fold (newT => t = newT, _ => ())
            Some(fst)
          }

          // this stream is "odd", i.e. we are always evaluating 1 step ahead
          // of what the client sees.  We could improve laziness by making it
          // "even", but it would be a little trickier, as `hasNext` would have
          // a forcing side-effect
          override def hasNext() = last.isDefined

          override def next() =
            last match {
              case Some(\/-((b, s))) =>
                val next = step(s)
                // The assumption here is that statefulMapConcat's implementation
                // will always read iterator to end before invoking on the next A
                next fold (newT => t = newT, _ => ())
                last = Some(next)
                \/-(b)
              case Some(et @ -\/(_)) =>
                last = None
                et
              case None =>
                throw new IllegalStateException("iterator read past end")
            }
        }
      }
  }
}

package com.daml.lf.engine.trigger

import scalaz.{-\/, \/, \/-}
import akka.NotUsed
import akka.stream.scaladsl.Flow

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

  def flatMapConcat[T, A, B](zero: T)(f: (T, A) => UnfoldState[T, B]): Flow[A, B, NotUsed] =
    Flow[A].statefulMapConcat { () =>
      var t = zero
      // statefulMapConcat only uses 'iterator'.  We preserve the Iterable's
      // immutability by making one strict reference to the 't' var at creation
      // time, meaning any later 'iterator' call uses the same start state, no matter
      // whether the above 't' has been updated
      a =>
        new Iterable[B] {
          private[this] val bs = f(t, a)
          import bs.step
          override def iterator = new Iterator[B] {
            private[this] var last: T \/ (B, bs.S) = step(bs.init)

            override def hasNext() = last.isRight

            override def next() =
              last fold (
                _ => throw new IllegalStateException("iterator read past end"), {
                  case (b, s) =>
                    last = step(s)
                    // The assumption here is that statefulMapConcat's implementation
                    // will always read iterator to end before invoking on the next A
                    last fold (newT => t = newT, _ => ())
                    b
                }
              )
          }
        }
    }
}

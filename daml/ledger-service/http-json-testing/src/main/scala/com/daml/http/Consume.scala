// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.stream.scaladsl.Sink
import org.scalactic.source
import scalaz.{~>, Functor}

import scala.concurrent.{ExecutionContext, Future}

/** This example script reads two elements unconditionally, checking whether
  * the first element is "good" and that the second element has `true` as its
  * first member, then performs a future, then drains any remaining elements
  * from the stream, yielding a result that uses elements of all three reads.
  *
  * {{{
  *   val mySyntax = Consume.syntax[Foo] // Foo = stream element type
  *   import mySyntax._
  *
  *   val sink: Sink[Foo, Future[Bar]] = Consume.interpret(for {
  *     a <- readOne
  *     if aGoodFoo(a) // abort if false
  *
  *     Foo(true, b) <- readOne // abort if pattern doesn't match
  *     _ <- liftF(Future { q(a, b) /* complete this future before continuing */ })
  *
  *     rest <- drain // Seq[Foo] of remainder; use only if you expect more elements
  *   } yield Bar(a, b, rest)
  * }}}
  */
sealed abstract class Consume[-T, +V]

object Consume {
  import scalaz.Free
  import scalaz.std.scalaFuture._

  type FCC[T, V] = Free[Consume[T, *], V]
  type Description = source.Position
  final case class Listen[-T, +V](f: T => V, desc: Description) extends Consume[T, V]
  final case class Drain[S, -T, +V](init: S, next: (S, T) => S, out: S => V) extends Consume[T, V]
  final case class Emit[+V](run: Future[V]) extends Consume[Any, V]

  /** Strictly speaking, this function really returns the following, these
    * are just passed to `Sink.foldAsync` for convenience:
    *
    * {{{
    *   {
    *     type O
    *     val init: O
    *     val step: (O, T) => Future[O]
    *     val out: O => Future[V]
    *   }
    * }}}
    */
  def interpret[T, V](steps: FCC[T, V])(implicit ec: ExecutionContext): Sink[T, Future[V]] =
    Sink
      .foldAsync(steps) { (steps, t: T) =>
        // step through steps until performing exactly one listen,
        // then step through any further steps until encountering
        // either the end or the next listen
        def go(steps: FCC[T, V], listened: Boolean): Future[FCC[T, V]] =
          steps.resume.fold(
            {
              case listen @ Listen(f, _) =>
                if (listened) Future successful (Free roll listen) else go(f(t), true)
              case drain: Drain[s, T, FCC[T, V]] =>
                Future successful Free.roll {
                  if (listened) drain
                  else drain.copy(init = drain.next(drain.init, t))
                }
              case Emit(run) => run flatMap (go(_, listened))
            },
            v =>
              if (listened) Future successful (Free point v)
              else
                Future.failed(
                  new IllegalStateException(
                    s"unexpected element $t, script already terminated with $v"
                  )
                ),
          )
        go(steps, false)
      }
      .mapMaterializedValue(_.flatMap(_.foldMap(Lambda[Consume[T, *] ~> Future] {
        case Listen(_, desc) =>
          Future.failed(
            new IllegalStateException(
              s"${describe(desc)}: script terminated early, expected another value"
            )
          )
        case Drain(init, _, out) => Future(out(init))
        case Emit(run) => run
      })))

  implicit def `consume functor`[T](implicit ec: ExecutionContext): Functor[Consume[T, *]] =
    new Functor[Consume[T, *]] {
      override def map[A, B](fa: Consume[T, A])(f: A => B): Consume[T, B] = fa match {
        case Listen(g, desc) => Listen(g andThen f, desc)
        case Drain(init, next, out) => Drain(init, next, out andThen f)
        case Emit(run) => Emit(run map f)
      }
    }

  private def describe(d: Description) = s"${d.fileName}:${d.lineNumber}"

  implicit final class `Consume Ops`[T, V](private val steps: FCC[T, V]) extends AnyVal {
    def withFilter(p: V => Boolean)(implicit pos: source.Position): Free[Consume[T, *], V] =
      steps flatMap { v =>
        if (p(v)) Free point v
        else
          Free liftF Emit(
            Future failed new IllegalStateException(
              s"${describe(pos)}: script cancelled by match error on $v"
            )
          )
      }
  }

  def syntax[T]: Syntax[T] = new Syntax

  final class Syntax[T] {
    def readOne(implicit pos: source.Position): FCC[T, T] = Free liftF Listen(identity, pos)
    def drain: FCC[T, Seq[T]] =
      Free liftF Drain(Nil, (acc: List[T], t) => t :: acc, (_: List[T]).reverse)
    def liftF[V](run: Future[V]): FCC[T, V] = Free liftF Emit(run)
    def point[V](v: V): FCC[T, V] = Free point v
  }
}

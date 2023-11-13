// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import cats.{Applicative, Eval, Traverse}

trait HasTraceContext {
  def traceContext: TraceContext
}

/** Wrapper for items that have a related trace context.
  * Intended for where the TraceContext cannot be passed explicitly (e.g. function types or pekko-streams).
  */
final case class Traced[+A](value: A)(implicit override val traceContext: TraceContext)
    extends HasTraceContext {
  def map[B](fn: A => B): Traced[B] = new Traced[B](fn(value))

  def mapWithTraceContext[B](fn: TraceContext => A => B): Traced[B] =
    Traced(withTraceContext(fn))

  def withTraceContext[B](fn: TraceContext => A => B): B = fn(traceContext)(value)

  override def toString: String = s"Traced($value)($traceContext)"
}

object Traced {
  def empty[A](x: A): Traced[A] = Traced(x)(TraceContext.empty)

  def fromPair[A](at: (A, TraceContext)): Traced[A] = {
    val (a, t) = at
    Traced(a)(t)
  }

  def withTraceContext[A <: HasTraceContext, B](traced: A)(fn: TraceContext => A => B): B =
    fn(traced.traceContext)(traced)

  /** Create a function expecting a Traced item from our usual implicit traceContext signature.
    * Typically used by: `Traced.lift(myMethod(_)(_))`
    */
  def lift[A, B](fn: (A, TraceContext) => B): Traced[A] => B =
    ta => fn(ta.value, ta.traceContext)

  implicit val traverseTraced: Traverse[Traced] = new Traverse[Traced] {
    override def traverse[G[_], A, B](
        traced: Traced[A]
    )(f: A => G[B])(implicit G: Applicative[G]): G[Traced[B]] = {
      G.map(f(traced.value)) { newValue =>
        traced.copy(value = newValue)(traced.traceContext)
      }
    }

    override def foldLeft[A, B](traced: Traced[A], b: B)(f: (B, A) => B): B = f(b, traced.value)
    override def foldRight[A, B](traced: Traced[A], lb: Eval[B])(
        f: (A, Eval[B]) => Eval[B]
    ): Eval[B] =
      f(traced.value, lb)
  }

}

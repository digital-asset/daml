// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import scala.language.{higherKinds, implicitConversions}

/** [[LoggingContext]] with a phantom type parameter representing what kind of
  * details are in it.  If a function that accepts a LoggingContext is supposed
  * to trust that the caller has already embedded all the relevant data that
  * would be passed as arguments into the context, then you could say a function
  * that accepts a [[LoggingContextOf]] will "trust, but verify" instead.
  *
  * You can pick a tag to represent each kind of data you want to appear in a
  * context.  The use of `+` means the tag language in `LoggingContextOf[Tag]`
  * reflects the subtyping relation built into Scala, and `Any` and `with` form
  * the zero and append of a commutative monoid of tags.
  */
object LoggingContextOf {

  def label[P]: label[P] = new label(0)
  final class label[P] private[LoggingContextOf] (private val ignored: Int) extends AnyVal

  def newLoggingContext[P, Z](label: label[P], kvs: Map[String, String])(
      f: LoggingContextOf[P] => Z): Z =
    LoggingContext.newLoggingContext(kvs)(lc => f(lc.extend[P]))

  def withEnrichedLoggingContext[P, A](label: label[P], kvs: Map[String, String])(
      implicit loggingContext: LoggingContextOf[A]): withEnrichedLoggingContext[P, A] =
    new withEnrichedLoggingContext(kvs, loggingContext.extend[P])

  final class withEnrichedLoggingContext[P, A] private[LoggingContextOf] (
      kvs: Map[String, String],
      loggingContext: LoggingContextOf[P with A]) {
    def run[Z](f: LoggingContextOf[P with A] => Z): Z =
      LoggingContext.withEnrichedLoggingContext(kvs)(f)(loggingContext)
  }

  sealed abstract class Module {
    type T[+P] <: LoggingContext
    private[Module] val isa: LoggingContext =:= T[Any]
    private[Module] def extend[P, A](tp: T[A]): T[P with A]
  }

  object Module {
    val Instance: Module = new Module {
      type T[+P] = LoggingContext

      override val isa = implicitly[T[Any] =:= T[Any]]
      override def extend[P](tp: T[P]) = tp
    }

    import Instance.T
    implicit final class ops[P](private val self: T[P]) extends AnyVal {
      def extend[P2]: T[P with P2] = Instance extend self
    }

    implicit def `untyped type is any`: LoggingContext =:= LoggingContextOf[Any] = Instance.isa

    implicit def `untyped is any`(lc: LoggingContext): T[Any] = `untyped type is any`(lc)
  }
}

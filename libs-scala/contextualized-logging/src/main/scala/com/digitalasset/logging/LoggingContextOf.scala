// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import com.daml.logging.entries.{LoggingEntries, LoggingEntry}

import scala.annotation.nowarn
import scala.language.implicitConversions

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
  *
  * A few, but not all, type-level implications of this:
  *
  *  - `LoggingContextOf[Foo with Bar]` is-a `LoggingContextOf[Foo]`
  *  - `LoggingContextOf[Foo with Bar]` is-a `LoggingContextOf[Bar]`
  *  - `LoggingContextOf[Elephant]` is-a `LoggingContextOf[Animal]`
  *  - `LoggingContext` is-a `LoggingContextOf[Any]`
  *
  * A context with a more specific scope will always be preferred in implicit
  * resolution.  So if you call a function demanding a `LoggingContextOf[Foo]`
  * and you have two implicits in scope, a `LoggingContextOf[Foo]` and a
  * `LoggingContextOf[Foo with Bar]` then the latter will be chosen, in
  * accordance with SLS §7.2, §6.26.3.  This fits well the "more context =
  * better than" overall philosophy of the contextualized-logging library.
  */
object LoggingContextOf {

  def label[P]: label[P] = new label(())
  final class label[P] private[LoggingContextOf] (private val ignored: Unit) extends AnyVal

  @nowarn("msg=parameter value label .* is never used") // Proxy only
  def newLoggingContext[P, Z](label: label[P], entries: LoggingEntry*)(
      f: LoggingContextOf[P] => Z
  ): Z =
    LoggingContext.newLoggingContext(LoggingEntries(entries: _*))(lc =>
      f((lc: LoggingContextOf[Any]).extend[P])
    )

  @nowarn("msg=parameter value label .* is never used") // Proxy only
  def withEnrichedLoggingContext[P, A](
      label: label[P],
      kvs: LoggingEntry*
  )(implicit loggingContext: LoggingContextOf[A]): withEnrichedLoggingContext[P, A] =
    new withEnrichedLoggingContext(LoggingEntries(kvs: _*), loggingContext.extend[P])

  final class withEnrichedLoggingContext[P, A] private[LoggingContextOf] (
      entries: LoggingEntries,
      loggingContext: LoggingContextOf[P with A],
  ) {
    def run[Z](f: LoggingContextOf[P with A] => Z): Z =
      LoggingContext.withEnrichedLoggingContextFrom(entries)(lc =>
        f((lc: LoggingContextOf[Any]).extend[P with A])
      )(loggingContext)
  }

  sealed abstract class Module {
    type T[+P] <: LoggingContext
    private[LoggingContextOf] val isa: LoggingContext =:= T[Any]
    private[LoggingContextOf] def extend[P, A](tp: T[A]): T[P with A]
  }

  object Module {
    val Instance: Module = new Module {
      type T[+P] = LoggingContext

      override private[LoggingContextOf] val isa = implicitly[T[Any] =:= T[Any]]
      override private[LoggingContextOf] def extend[P, A](tp: T[P]) = tp
    }

    import Instance.T
    implicit final class ops[P](private val self: T[P]) extends AnyVal {
      def extend[P2]: T[P with P2] = Instance extend self
    }

    implicit def `untyped type is any`: LoggingContext =:= LoggingContextOf[Any] = Instance.isa

    implicit def `untyped is any`(lc: LoggingContext): T[Any] = `untyped type is any`(lc)
  }
}

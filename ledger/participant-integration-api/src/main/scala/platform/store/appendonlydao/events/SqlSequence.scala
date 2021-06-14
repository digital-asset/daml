// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import scalaz.{-\/, Free, Functor, \/-}

import java.sql.Connection

object SqlSequence {

  /** A sequence of `SimpleSql`s, terminating in A. */
  type T[A] = Free[Element, A]

  // this representation is just trampolined Reader, but exposing that
  // would be unsound because it is _not_ distributive, and anyway
  // we may want to make the representation more explicit for more complex
  // analysis (e.g. applying polymorphic transforms to all contained SimpleSqls...)
  final class Element[+A] private[SqlSequence] (private[SqlSequence] val run: Connection => A)

  object Element {
    implicit final class syntax[A](private val self: T[A]) extends AnyVal {
      def executeSql(implicit conn: Connection): A = {
        @annotation.tailrec
        def go(self: T[A]): A =
          self.resume match {
            case -\/(elt) => go(elt.run(conn))
            case \/-(a) => a
          }
        go(self)
      }
    }

    implicit val covariant: Functor[Element] = new Functor[Element] {
      override def map[A, B](fa: Element[A])(f: A => B) = new Element(run = fa.run andThen f)
    }
  }

  // TODO append-only: This probably defeats the purpose, only needed to limit the impact of the SQL query abstraction change. Consider to remove SqlSequence.
  def plainQuery[A](query: Connection => A): T[A] =
    Free liftF new Element(query)

  def point[A](a: A): T[A] = Free point a
}

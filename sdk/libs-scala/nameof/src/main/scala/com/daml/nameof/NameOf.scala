// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nameof

trait NameOf {
  import scala.language.experimental.macros

  /** Obtain the full qualified identifier name of a method as a constant string.
    *
    * Example usage:
    * {{{
    *   package foo.bar.spam
    *   case class Ham() {
    *      def ham() = qualifiedNameOfCurrentFunc()
    *    }
    *
    *   Ham().ham() == "foo.bar.spam.Ham.ham"
    * }}}
    */
  def qualifiedNameOfCurrentFunc: String = macro NameOfImpl.qualifiedNameOfCurrentFunc

  /** Alias for qualifiedNameOfCurrentFunc */
  def functionFullName: String = macro NameOfImpl.qualifiedNameOfCurrentFunc

  /** Obtain the full qualified identifier of the given expression as a constant string.
    *
    * Example usage:
    * {{{
    *   qualifiedNameOf(None) == "scala.None"
    *   qualifiedNameOf(Option.empty) == "scala.Option.empty"
    * }}}
    */
  def qualifiedNameOf(x: Any): String = macro NameOfImpl.qualifiedNameOf

  /** Obtain the full qualified identifier of the given function if applied to a value of type `A`.
    * Use this method instead of [[qualifiedNameOf]] when you do not have an value of type `A` at hand.
    *
    * Example uasage:
    * {{{
    *   qualifiedNameOfMember[String](_.strip()) == "java.lang.String.strip"
    *   qualifiedNameOfMember[Option[_]](_.map(_ => ???)) == "scala.Option.map"
    * }}}
    */
  def qualifiedNameOfMember[A](func: A => Any): String = macro NameOfImpl.qualifiedNameOfMember[A]
}
object NameOf extends NameOf

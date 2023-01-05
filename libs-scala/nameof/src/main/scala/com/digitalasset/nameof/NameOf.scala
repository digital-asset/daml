// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  def qualifiedNameOfCurrentFunc: String = macro NameOfImpl.nameOf
}
object NameOf extends NameOf

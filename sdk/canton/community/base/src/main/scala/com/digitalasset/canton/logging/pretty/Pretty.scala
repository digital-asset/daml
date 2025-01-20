// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import com.digitalasset.canton.util.{ErrorUtil, ShowUtil}
import pprint.{PPrinter, Tree}

/** Type class indicating that pretty printing is enabled for type `T`.
  *
  * See `PrettyPrintingTest` for examples on how to create instances.
  */
@FunctionalInterface // See https://github.com/scala/bug/issues/11644
trait Pretty[-T] {

  /** Yields a syntax tree which is used as a basis for the String representation.
    */
  def treeOf(t: T): Tree
}

/** The companion object collects everything you might need for implementing [[Pretty]] instances.
  *
  * Import this as follows:
  * <pre>
  * implicit val prettyMyClass: Pretty[MyClass] = {
  *   import Pretty._
  *   ...
  * }
  * </pre>
  */
object Pretty extends ShowUtil with PrettyUtil with PrettyInstances {

  val DefaultWidth = 200
  val DefaultHeight = 100
  val DefaultIndent = 2
  val DefaultEscapeUnicode: Boolean = false
  val DefaultShowFieldNames: Boolean = false

  def apply[A](implicit pretty: Pretty[A]): Pretty[A] = pretty

  /** Default PPrinter used to implement `toString` and `show` methods.
    */
  val DefaultPprinter: PPrinter =
    PPrinter.BlackWhite.copy(
      defaultWidth = DefaultWidth,
      defaultHeight = DefaultHeight,
      defaultIndent = DefaultIndent,
      additionalHandlers = { case p: PrettyPrinting =>
        p.toTree
      },
    )

  /** Convenience methods for [[Pretty]] types.
    */
  implicit class PrettyOps[T: Pretty](value: T) {

    /** Yields a readable string representation based on a configurable [[pprint.PPrinter]].
      */
    final def toPrettyString(pprinter: PPrinter = DefaultPprinter): String = {
      try {
        pprinter.copy(additionalHandlers = { case p: Tree => p })(toTree).toString
      } catch {
        case err: IllegalArgumentException =>
          ErrorUtil.messageWithStacktrace(err)
      }
    }

    /** The tree representation of `value`.
      */
    final def toTree: Tree = implicitly[Pretty[T]].treeOf(value)
  }
}

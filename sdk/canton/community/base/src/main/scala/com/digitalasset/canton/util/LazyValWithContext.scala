// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import scala.concurrent.blocking

/** "Implements" a `lazy val` field whose initialization expression can refer to implicit context information of type `Context`.
  * The "val" is initialized upon the first call to [[get]], using the context information supplied for this call,
  * like a `lazy val`.
  *
  * Instead of a plain lazy val field without context
  * <pre>class C { lazy val f: T = initializer }</pre>
  * use the following code to pass in a `Context`:
  * <pre>
  * class C {
  *   private[this] val _f: LazyValWithContext[T, Context] = new LazyValWithContext[T, Context](context => initializer)
  *   def f(implicit context: Context): T = _f.get
  * }
  * </pre>
  *
  * This class implements the same scheme as how the Scala 2.13 compiler implements `lazy val`s,
  * as explained on https://docs.scala-lang.org/sips/improved-lazy-val-initialization.html (version V1)
  * along with its caveats.
  *
  * @see TracedLazyVal To be used when the initializer wants to log something using the logger of the surrounding class
  * @see ErrorLoggingLazyVal To be used when the initializer wants to log errors using the logger of the caller
  */
final class LazyValWithContext[T, Context](initialize: Context => T) {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var bitmap_0: Boolean = false

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private var value_0: T = _

  private def value_lzycompute(context: Context): T = {
    blocking {
      this.synchronized {
        if (!bitmap_0) {
          value_0 = initialize(context)
          bitmap_0 = true
        }
      }
    }
    value_0
  }

  def get(implicit context: Context): T =
    if (bitmap_0) value_0 else value_lzycompute(context)
}

trait LazyValWithContextCompanion[Context] {
  def apply[T](initialize: Context => T): LazyValWithContext[T, Context] =
    new LazyValWithContext[T, Context](initialize)
}

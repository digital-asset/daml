// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object TryUtil {

  /** Constructs a `Try` using the by-name parameter.  This
    * method will ensure any non-fatal exception and [[java.lang.InterruptedException]] is caught and a
    * `Failure` object is returned.
    */
  def tryCatchInterrupted[A](r: => A): Try[A] =
    try Success(r)
    catch {
      case e: InterruptedException => Failure(e)
      case NonFatal(e) => Failure(e)
    }

  implicit final class ForFailedOps[A](private val a: Try[A]) extends AnyVal {
    @inline
    def forFailed(f: Throwable => Unit): Unit = a.fold(f, _ => ())
  }

}

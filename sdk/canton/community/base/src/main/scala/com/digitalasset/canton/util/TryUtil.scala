// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import java.util.concurrent.CompletionException
import scala.annotation.tailrec
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

    @inline
    def valueOr[B >: A](f: Throwable => B): B = a.fold(f, identity)
  }

  /** Unwraps all [[java.util.concurrent.CompletionException]] from a failure and
    * leaves only the wrapped causes (unless there is no such cause)
    */
  def unwrapCompletionException[A](x: Try[A]): Try[A] = x match {
    case _: Success[_] => x
    case Failure(ex) =>
      val stripped = stripCompletionException(ex)
      if (stripped eq ex) x else Failure(stripped)
  }

  @tailrec private def stripCompletionException(throwable: Throwable): Throwable = throwable match {
    case ce: CompletionException =>
      if (ce.getCause != null) stripCompletionException(ce.getCause)
      else ce
    case _ => throwable
  }
}

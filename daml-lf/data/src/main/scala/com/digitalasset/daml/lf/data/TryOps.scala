// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.util.{Failure, Success, Try}

private[daml] object TryOps {
  def sequence[A](list: List[Try[A]]): Try[List[A]] = {
    val zero: Try[List[A]] = Success(List.empty[A])
    list.foldRight(zero)((a, as) => map2(a, as)(_ :: _))
  }

  def map2[A, B, C](ta: Try[A], tb: Try[B])(f: (A, B) => C): Try[C] =
    for {
      a <- ta
      b <- tb
    } yield f(a, b)

  @deprecated("use scala.util.Using", since = "1.16.0")
  object Bracket {

    /** The following description borrowed from https://hackage.haskell.org/package/exceptions-0.10.1/docs/Control-Monad-Catch.html#v:bracket
      * {{{
      * Generalized abstracted pattern of safe resource acquisition and release in the face of errors.
      * The first action "acquires" some value, which is "released" by the second action at the end.
      * The third action "uses" the value and its result is the result of the bracket.
      *
      * If an error is thrown during the use, the release still happens before the error is rethrown.
      * }}}
      *
      * If cleanup fails, it's error returned. If you don't like this behavior, don't return a Failure from the cleanup.
      * If both flatMap and cleanup fail, flatMap's Failure returned.
      *
      * Examples:
      *
      * {{{
      *  private def parseDalfNamesFromManifest(darFile: ZipFile): Try[Dar[String]] =
      *    bracket(Try(darFile.getInputStream(manifestEntry)))(close)
      *      .flatMap(is => readDalfNamesFromManifest(is))
      *
      *  private def parseOne(f: ZipFile)(s: String): Try[A] =
      *    bracket(getZipEntryInputStream(f, s))(close).flatMap(parseDalf)
      *
      *  private def close(is: InputStream): Try[Unit] = Try(is.close())
      * }}}
      *
      * @see https://hackage.haskell.org/package/exceptions-0.10.1/docs/Control-Monad-Catch.html#v:bracket
      * @param fa   acquired resource
      * @param cleanup  cleanup function that releases the acquired resource
      */
    def bracket[A, B](fa: Try[A])(cleanup: A => Try[B]): Bracket[A, B] = new Bracket(fa, cleanup)

    final class Bracket[A, B](fa: Try[A], cleanup: A => Try[B]) {
      def flatMap[C](f: A => Try[C]): Try[C] = {
        val fc = fa.flatMap(a => f(a))
        val fb = fa.flatMap(a => cleanup(a))
        (fc, fb) match {
          case (Success(_), Success(_)) => fc
          case (e @ Failure(_), _) => e
          case (Success(_), Failure(e)) => Failure(e)
        }
      }

      def map[C](f: A => C): Try[C] = flatMap(a => Try(f(a)))
    }
  }
}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.regex.Pattern

import ai.x.diff.DiffShow
import com.daml.grpc.{GrpcException, GrpcStatus}
import io.grpc.Status

import scala.language.higherKinds
import scala.util.control.NonFatal

object Assertions extends DiffExtensions {
  def fail(message: String): Nothing =
    throw new AssertionError(message)

  def fail(message: String, cause: Throwable): Nothing =
    throw new AssertionError(message, cause)

  def assertLength[A, F[_] <: Seq[_]](context: String, length: Int, as: F[A]): F[A] = {
    assert(as.length == length, s"$context: expected $length item(s), got ${as.length}")
    as
  }

  def assertSingleton[A](context: String, as: Seq[A]): A =
    assertLength(context, 1, as).head

  def assertEquals[T: DiffShow](context: String, actual: T, expected: T): Unit = {
    val diff = DiffShow.diff(actual, expected)
    if (!diff.isIdentical)
      throw AssertionErrorWithPreformattedMessage(
        diff.string,
        s"$context: two objects are supposed to be equal but they are not",
      )
  }

  /** Match the given exception against a status code and a regex for the expected message.
      Succeeds if the exception is a GrpcException with the expected code and
      the regex matches some part of the message or there is no message and the pattern is
      None.
    */
  def assertGrpcError(t: Throwable, expectedCode: Status.Code, optPattern: Option[Pattern]): Unit =
    (t, optPattern) match {
      case (GrpcException(GrpcStatus(`expectedCode`, Some(msg)), _), Some(pattern)) =>
        if (pattern.matcher(msg).find()) {
          ()
        } else {
          fail(s"Error message did not contain [$pattern], but was [$msg].")
        }
      // None both represents pattern that we do not care about as well as
      // exceptions that have no message.
      case (GrpcException(GrpcStatus(`expectedCode`, _), _), None) => ()
      case (GrpcException(GrpcStatus(code, _), _), _) =>
        fail(s"Expected code [$expectedCode], but got [$code].")
      case (NonFatal(e), _) =>
        fail("Exception is neither a StatusRuntimeException nor a StatusException", e)
    }

  /** non-regex overload for assertGrpcError which just does a substring check.
    */
  def assertGrpcError(t: Throwable, expectedCode: Status.Code, pattern: String): Unit = {
    assertGrpcError(
      t,
      expectedCode,
      if (pattern.isEmpty) None else Some(Pattern.compile(Pattern.quote(pattern))))
  }
}

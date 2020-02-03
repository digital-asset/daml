// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import ai.x.diff.DiffShow
import com.digitalasset.grpc.{GrpcException, GrpcStatus}
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

  def assertGrpcError(t: Throwable, expectedCode: Status.Code, pattern: String): Unit =
    t match {
      case GrpcException(GrpcStatus(`expectedCode`, Some(msg)), _) if msg.contains(pattern) =>
        ()
      case GrpcException(GrpcStatus(`expectedCode`, None), _) if pattern.isEmpty =>
        ()
      case GrpcException(GrpcStatus(`expectedCode`, description), _) =>
        fail(s"Error message did not contain [$pattern], but was [$description].")
      case GrpcException(GrpcStatus(code, _), _) =>
        fail(s"Expected code [$expectedCode], but got [$code].")
      case NonFatal(e) =>
        fail("Exception is neither a StatusRuntimeException nor a StatusException", e)
    }
}

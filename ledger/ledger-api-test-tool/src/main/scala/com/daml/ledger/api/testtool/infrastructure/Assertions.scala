// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import ai.x.diff.DiffShow
import io.grpc.{Status, StatusException, StatusRuntimeException}

import scala.language.higherKinds

object Assertions extends DiffExtensions {
  def fail(message: => String): Nothing =
    throw new AssertionError(message)

  def assertLength[A, F[_] <: Seq[_]](context: String, length: Int, as: F[A]): F[A] = {
    assert(as.length == length, s"$context: expected $length item(s), got ${as.length}")
    as
  }

  def assertSingleton[A](context: String, as: Seq[A]): A =
    assertLength(context, 1, as).head

  def assertEquals[T: DiffShow](context: String, actual: T, expected: T): Unit = {
    val diff = DiffShow.diff(actual, expected)
    if (!diff.isIdentical)
      throw new AssertionErrorWithPreformattedMessage(
        diff.string,
        s"$context: two objects are supposed to be equal but they are not")
  }

  def assertGrpcError(t: Throwable, expectedCode: Status.Code, pattern: String): Unit = {

    val (actualCode, message) = t match {
      case sre: StatusRuntimeException => (sre.getStatus.getCode, sre.getStatus.getDescription)
      case se: StatusException => (se.getStatus.getCode, se.getStatus.getDescription)
      case _ =>
        throw new AssertionError(
          "Exception is neither a StatusRuntimeException nor a StatusException")
    }
    assert(actualCode == expectedCode, s"Expected code [$expectedCode], but got [$actualCode].")
    // Note: Status.getDescription() is nullable, map missing descriptions to an empty string
    val nonNullMessage = Option(message).getOrElse("")
    assert(
      nonNullMessage.contains(pattern),
      s"Error message did not contain [$pattern], but was [$nonNullMessage].")
  }
}

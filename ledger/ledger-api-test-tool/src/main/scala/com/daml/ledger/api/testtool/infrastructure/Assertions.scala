// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.regex.Pattern

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.timer.RetryStrategy
import com.google.rpc.ErrorInfo
import io.grpc.Status
import io.grpc.protobuf.StatusProto
import munit.{ComparisonFailException, Assertions => MUnit}

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

object Assertions {
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

  def assertEquals[T](context: String, actual: T, expected: T): Unit = {
    try {
      MUnit.assertEquals(actual, expected, context)
    } catch {
      case e: ComparisonFailException =>
        throw AssertionErrorWithPreformattedMessage(
          e.message,
          s"$context: two objects are supposed to be equal but they are not",
        )
    }
  }

  /** non-regex overload for assertGrpcError which just does a substring check.
    */
  def assertGrpcError(
      t: Throwable,
      expectedCode: Status.Code,
      exceptionMessageSubString: String,
      checkDefiniteAnswerMetadata: Boolean = false,
  ): Unit =
    assertGrpcError(
      t,
      expectedCode,
      if (exceptionMessageSubString.isEmpty) None
      else Some(Pattern.compile(Pattern.quote(exceptionMessageSubString))),
      checkDefiniteAnswerMetadata,
    )

  /** Match the given exception against a status code and a regex for the expected message.
    *      Succeeds if the exception is a GrpcException with the expected code and
    *      the regex matches some part of the message or there is no message and the pattern is
    *      None.
    */
  @tailrec
  def assertGrpcError(
      t: Throwable,
      expectedCode: Status.Code,
      optPattern: Option[Pattern],
      checkDefiniteAnswerMetadata: Boolean,
  ): Unit =
    (t, optPattern) match {
      case (RetryStrategy.FailedRetryException(cause), _) =>
        assertGrpcError(cause, expectedCode, optPattern, checkDefiniteAnswerMetadata)
      case (
            exception @ GrpcException(GrpcStatus(`expectedCode`, Some(message)), _),
            Some(pattern),
          ) =>
        assertMatches(message, pattern)
        if (checkDefiniteAnswerMetadata) {
          assertDefiniteAnswer(exception)
        }
      // None both represents pattern that we do not care about as well as
      // exceptions that have no message.
      case (GrpcException(GrpcStatus(`expectedCode`, _), _), None) => ()
      case (GrpcException(GrpcStatus(code, _), _), _) =>
        fail(s"Expected code [$expectedCode], but got [$code].")
      case (_, _) =>
        fail("Exception is neither a StatusRuntimeException nor a StatusException", t)
    }

  private def assertMatches(message: String, pattern: Pattern): Unit = {
    if (pattern.matcher(message).find()) {
      ()
    } else {
      fail(s"Error message did not contain [$pattern], but was [$message].")
    }
  }

  private def assertDefiniteAnswer(exception: Exception): Unit = {
    val details = StatusProto.fromThrowable(exception).getDetailsList.asScala
    val metadata = details
      .find(_.is(classOf[ErrorInfo]))
      .map { any =>
        val errorInfo = any.unpack(classOf[ErrorInfo])
        errorInfo.getMetadataMap
      }
      .getOrElse {
        fail(
          s"The error did not contain a definite answer. Details were: ${details.mkString("[", ", ", "]")}"
        )
      }
    val value = metadata.get("definite_answer")
    if (value == null) {
      fail(s"The error did not contain a definite answer. Metadata was: [$metadata]")
    }
    if (!Set("true", "false").contains(value.toLowerCase)) {
      fail(s"The error contained an invalid definite answer: [$value]")
    }
  }

  /** Allows for assertions with more information in the error messages. */
  implicit def futureAssertions[T](future: Future[T]): FutureAssertions[T] =
    new FutureAssertions[T](future)
}

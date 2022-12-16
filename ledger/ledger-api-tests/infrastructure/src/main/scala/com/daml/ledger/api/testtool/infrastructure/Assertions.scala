// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.regex.Pattern

import com.daml.error.ErrorCode
import com.daml.error.utils.ErrorDetails
import com.daml.timer.RetryStrategy
import com.google.rpc.ErrorInfo
import io.grpc.protobuf.StatusProto
import io.grpc.StatusRuntimeException
import munit.{ComparisonFailException, Assertions => MUnit}

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.Try

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

  def assertEquals[T](actual: T, expected: T): Unit = {
    try {
      MUnit.assertEquals(actual, expected)
    } catch {
      case e: ComparisonFailException =>
        throw AssertionErrorWithPreformattedMessage(
          e.message,
          s"two objects are supposed to be equal but they are not",
        )
    }
  }

  def assertSameElements[T](actual: Iterable[T], expected: Iterable[T]): Unit = {
    assert(
      actual.toSet == expected.toSet,
      s"Actual |${actual.mkString(", ")}| should have the same elements as (expected): |${expected.mkString(", ")}|",
    )
  }

  def assertIsEmpty(actual: Iterable[_]): Unit = {
    assertSameElements(actual, Seq.empty)
  }

  def assertGrpcErrorOneOf(t: Throwable, errors: ErrorCode*): Unit = {
    val hasErrorCode =
      errors.map(errorCode => Try(assertGrpcError(t, errorCode, None))).exists(_.isSuccess)
    if (!hasErrorCode)
      fail(s"gRPC failure did not contain one of the expected error codes $errors.", t)
  }

  def assertGrpcError(
      t: Throwable,
      errorCode: ErrorCode,
      exceptionMessageSubstring: Option[String],
      checkDefiniteAnswerMetadata: Boolean = false,
      additionalErrorAssertions: Throwable => Unit = _ => (),
  ): Unit =
    assertGrpcErrorRegex(
      t,
      errorCode,
      exceptionMessageSubstring
        .map(msgSubstring => Pattern.compile(Pattern.quote(msgSubstring))),
      checkDefiniteAnswerMetadata,
      additionalErrorAssertions,
    )

  /** Match the given exception against a status code and a regex for the expected message.
    * Succeeds if the exception is a GrpcException with the expected code and
    * the regex matches some part of the message or there is no message and the pattern is
    * None.
    */
  @tailrec
  def assertGrpcErrorRegex(
      t: Throwable,
      errorCode: ErrorCode,
      optPattern: Option[Pattern],
      checkDefiniteAnswerMetadata: Boolean = false,
      additionalErrorAssertions: Throwable => Unit = _ => (),
  ): Unit =
    t match {
      case RetryStrategy.FailedRetryException(cause) =>
        assertGrpcErrorRegex(
          cause,
          errorCode,
          optPattern,
          checkDefiniteAnswerMetadata,
          additionalErrorAssertions,
        )
      case exception: StatusRuntimeException =>
        optPattern.foreach(assertMatches(exception.getMessage, _))
        assertErrorCode(exception, errorCode)
        if (checkDefiniteAnswerMetadata) assertDefiniteAnswer(exception)
        additionalErrorAssertions(exception)
      case _ =>
        fail("Exception is not a StatusRuntimeException", t)
    }

  private def assertMatches(message: String, pattern: Pattern): Unit =
    if (pattern.matcher(message).find()) {
      ()
    } else {
      fail(s"Error message did not contain [$pattern], but was [$message].")
    }

  private def assertDefiniteAnswer(exception: Exception): Unit = {
    val definitiveAnswer = extractErrorInfoMetadataValue(exception, "definite_answer")
    if (!Set("true", "false").contains(definitiveAnswer.toLowerCase)) {
      fail(s"The error contained an invalid definite answer: [$definitiveAnswer]")
    }
  }

  def extractErrorInfoMetadataValue(exception: Throwable, key: String): String = {
    val metadata = extractErrorInfoMetadata(exception)
    metadata.get(key) match {
      case Some(value) =>
        value
      case None =>
        fail(
          s"The error metadata did not contain the key $key. Metadata was: [$metadata]",
          exception,
        )
    }
  }

  def extractErrorInfoMetadata(exception: Throwable): Map[String, String] =
    extractErrorInfoMetadata(StatusProto.fromThrowable(exception))

  def extractErrorInfoMetadata(status: com.google.rpc.Status): Map[String, String] = {
    val details = status.getDetailsList.asScala
    details
      .find(_.is(classOf[ErrorInfo]))
      .map { any =>
        val errorInfo = any.unpack(classOf[ErrorInfo])
        errorInfo.getMetadataMap.asScala.toMap
      }
      .getOrElse {
        Map.empty
      }
  }

  def assertErrorCode(
      statusRuntimeException: StatusRuntimeException,
      expectedErrorCode: ErrorCode,
  ): Unit = {
    val status = StatusProto.fromThrowable(statusRuntimeException)

    val expectedStatusCode = expectedErrorCode.category.grpcCode
      .map(_.value())
      .getOrElse(
        throw new RuntimeException(
          s"Errors without grpc code cannot be asserted on the Ledger API. Expected error: $expectedErrorCode"
        )
      )
    val expectedErrorId = expectedErrorCode.id
    val expectedRetryability = expectedErrorCode.category.retryable.map(_.duration)

    val actualStatusCode = status.getCode
    val actualErrorDetails = ErrorDetails.from(status.getDetailsList.asScala.toSeq)
    val actualErrorId = actualErrorDetails
      .collectFirst { case err: ErrorDetails.ErrorInfoDetail => err.errorCodeId }
      .getOrElse(fail(s"Actual error id is not defined. Actual error: $statusRuntimeException"))
    val actualRetryability = actualErrorDetails
      .collectFirst { case err: ErrorDetails.RetryInfoDetail => err.duration }

    if (actualErrorId != expectedErrorId)
      fail(
        s"Actual error id ($actualErrorId) does not match expected error id ($expectedErrorId}. Actual error: $statusRuntimeException"
      )

    Assertions.assertEquals(
      "gRPC error code mismatch",
      actualStatusCode,
      expectedStatusCode,
    )

    Assertions.assertEquals(
      s"Error retryability details mismatch",
      actualRetryability,
      expectedRetryability,
    )
  }

  def assertDefined[T](option: Option[T], errorMessage: String): T = {
    assert(option.isDefined, errorMessage)
    option.get
  }

  /** Allows for assertions with more information in the error messages. */
  implicit def futureAssertions[T](future: Future[T]): FutureAssertions[T] =
    new FutureAssertions[T](future)
}

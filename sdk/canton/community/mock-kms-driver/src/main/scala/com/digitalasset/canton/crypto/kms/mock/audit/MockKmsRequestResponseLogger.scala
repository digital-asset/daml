// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.mock.audit

import cats.data.EitherT
import com.digitalasset.canton.crypto.kms.audit.KmsRequestResponseLogger
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*

import java.util.UUID
import scala.concurrent.ExecutionContext

class MockKmsRequestResponseLogger(
    auditLogging: Boolean,
    override val loggerFactory: NamedLoggerFactory,
) extends KmsRequestResponseLogger
    with NamedLogging {

  def withLogging[A](
      requestMsg: String,
      responseMsg: A => String,
  )(
      f: => EitherT[FutureUnlessShutdown, String, A]
  )(implicit tc: TraceContext, ec: ExecutionContext): EitherT[FutureUnlessShutdown, String, A] =
    if (!auditLogging) f
    else {
      val requestId = UUID.randomUUID().toString
      logger.info(s"Sending request [$requestId]: $requestMsg.")
      f.thereafter {
        case scala.util.Success(UnlessShutdown.Outcome(Right(result))) =>
          logger.info(s"Received response ${responseMsg(result)}. Original request [$requestId]")
        case scala.util.Success(UnlessShutdown.Outcome(Left(kmsError))) =>
          logger.warn(s"Request $requestId failed with: $kmsError")
        case scala.util.Success(_: AbortedDueToShutdown) =>
          logger.info(s"Request $requestId aborted due to shutdown.")
        case scala.util.Failure(throwable) =>
          logger.warn(s"Request $requestId failed", throwable)
      }
    }

}

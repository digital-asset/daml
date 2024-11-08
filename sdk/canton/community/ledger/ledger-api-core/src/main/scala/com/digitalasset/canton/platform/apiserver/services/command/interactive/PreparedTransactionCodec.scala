// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.Applicative
import cats.syntax.either.*
import com.daml.error.ContextualizedErrorLogger
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import io.scalaland.chimney.partial.Result

import scala.concurrent.Future

object PreparedTransactionCodec {
  implicit val chimneyResultApplicative: Applicative[Result] = new Applicative[Result] {
    override def pure[A](x: A): Result[A] = Result.fromValue(x)
    override def ap[A, B](ff: Result[A => B])(fa: Result[A]): Result[B] = ff.flatMap(fa.map)
  }

  // Convenience methods to deal with chimney Result values
  implicit private[interactive] class EnhancedChimneyResult[A](val result: Result[A])
      extends AnyVal {

    /** Converts a chimney Result to a Future.
      * In the result is a failure, detailed causes get logged at debug level, and a failed Future with a StatusRuntimeException
      * is returned, containing only the high level reason of the failure.
      */
    def toFutureWithLoggedFailures(description: String, logger: TracedLogger)(implicit
        errorLoggingContext: ContextualizedErrorLogger,
        traceContext: TraceContext,
    ): Future[A] = Future.fromTry {
      result.asEither
        .leftMap { err =>
          logger.info(s"$description: ${err.errors.mkString("\n")}")
          s"$description: ${err.errors.mkString("\n")}"
        }
        .leftMap(CommandExecutionErrors.InteractiveSubmissionPreparationError.Reject(_))
        .leftMap(_.asGrpcError)
        .toTry
    }
  }

  implicit private[interactive] class EnhancedEitherString[A](val either: Either[String, A])
      extends AnyVal {

    /** Converts an Either[String, A] to a Result[A]
      */
    def toResult: Result[A] = Result.fromEither(either.leftMap(Result.Errors.fromString))
  }

  implicit private[interactive] class EnhancedParsingResult[A](val parsingResult: ParsingResult[A])
      extends AnyVal {

    /** Converts a ParsingResult[A] to a Result[A]
      */
    def toResult: Result[A] = parsingResult.leftMap(_.message).toResult
  }
}

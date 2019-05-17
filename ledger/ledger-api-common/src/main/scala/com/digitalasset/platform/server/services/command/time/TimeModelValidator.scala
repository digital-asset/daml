// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.command.time

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.v1.TimeModelChecker
import com.digitalasset.platform.server.api.validation.ErrorFactories
import com.digitalasset.platform.services.time.{TimeModel, TimeModelChecker}
import io.grpc.Status

import scala.util.{Failure, Success, Try}

/**
  * Wraps a TimeModel and validates times, returning the appropriate exceptions.
  */
final case class TimeModelValidator(model: TimeModel) extends ErrorFactories {

  private val timeModelChecker = TimeModelChecker(model)

  /**
    * Wraps [[model.checkTtl]] with a StatusRuntimeException wrapper.
    */
  def checkTtl(givenLedgerEffectiveTime: Instant, givenMaximumRecordTime: Instant): Try[Unit] = {
    if (timeModelChecker.checkTtl(givenLedgerEffectiveTime, givenMaximumRecordTime))
      Success(())
    else {
      val givenTtl = Duration.between(givenLedgerEffectiveTime, givenMaximumRecordTime)
      Failure(
        invalidArgument(
          s"Command TTL (the difference between ledger effective time and maximum record time) $givenTtl is " +
            s"out of bounds. Min: ${model.minTtl}. Max: ${model.maxTtl}. " +
            s"Client may attempt resubmission with a value that falls within that interval."))
    }
  }

  /**
    * Wraps [[model.checkLet]] with a GslError wrapper. Asserts that the TTL is valid.
    */
  def checkLet(
      currentTime: Instant,
      givenLedgerEffectiveTime: Instant,
      givenMaximumRecordTime: Instant,
      commandId: String,
      applicationId: String): Try[Unit] =
    for {
      _ <- checkTtl(givenLedgerEffectiveTime, givenMaximumRecordTime)
      _ <- if (timeModelChecker.checkLet(
          currentTime,
          givenLedgerEffectiveTime,
          givenMaximumRecordTime))
        Success(())
      else
        Failure(
          Status.ABORTED
            .withDescription(
              toStatusMessage(
                Map(
                  "currentTime" -> currentTime.toString,
                  "ledgerEffectiveTime" -> givenLedgerEffectiveTime.toString,
                  // The lower and upper bound here is the window that *now* must fall into.
                  // Confusingly, this window is the inverse of the window in the time specification,
                  // which specifies where ledgerEffectiveTime must fall relative to now.
                  "lowerBound" -> givenLedgerEffectiveTime
                    .minus(model.futureAcceptanceWindow)
                    .toString,
                  "upperBound" -> givenMaximumRecordTime.toString
                )
              ))
            .asRuntimeException()
        )
    } yield ()

  private def toStatusMessage(params: Map[String, String]): String =
    params
      .map { case (k, v) => s"${k}:${v}" }
      .mkString(s"TRANSACTION_OUT_OF_TIME_WINDOW: [", ", ", "]")

}

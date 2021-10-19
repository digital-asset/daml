// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.IntegrityChecker.ComparisonFailureException
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.UpdateNormalizer.normalize
import com.daml.ledger.participant.state.v2.Update
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

trait StateUpdateComparison {
  def compare()(implicit loggingContext: LoggingContext): Future[Unit]
}

final class ReadServiceStateUpdateComparison(
    expectedReadService: ReplayingReadService,
    actualReadService: ReplayingReadService,
    expectedUpdateNormalizers: Iterable[UpdateNormalizer],
    actualUpdateNormalizers: Iterable[UpdateNormalizer],
    pairwiseUpdateNormalizers: Iterable[PairwiseUpdateNormalizer],
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
) extends StateUpdateComparison {

  import ReadServiceStateUpdateComparison._

  def compare()(implicit loggingContext: LoggingContext): Future[Unit] = {
    println("Comparing expected and actual state updates.".white)
    if (expectedReadService.updateCount() != actualReadService.updateCount()) {
      Future.failed(
        new ComparisonFailureException(
          s"Expected ${expectedReadService.updateCount()} state updates but got ${actualReadService.updateCount()}."
        )
      )
    } else {
      expectedReadService
        .stateUpdates(None)
        .zip(actualReadService.stateUpdates(None))
        .mapAsync(1) { case ((expectedOffset, expectedUpdate), (actualOffset, actualUpdate)) =>
          println(s"Comparing offset $expectedOffset...")
          Future.sequence(
            Seq(
              compareOffsets(expectedOffset, actualOffset),
              compareUpdates(
                expectedUpdate,
                actualUpdate,
                expectedUpdateNormalizers,
                actualUpdateNormalizers,
                pairwiseUpdateNormalizers,
              ),
            )
          )
        }
        .runWith(Sink.fold(0)((n, _) => n + 1))
        .map { counter =>
          println(s"Successfully compared $counter state updates.".green)
          println()
        }
    }
  }
}

object ReadServiceStateUpdateComparison {

  private def compareOffsets(expected: Offset, actual: Offset): Future[Unit] =
    if (expected != actual) {
      Future.failed(new ComparisonFailureException(s"Expected offset $expected but got $actual."))
    } else {
      Future.unit
    }

  private[integritycheck] def compareUpdates(
      expectedUpdate: Update,
      actualUpdate: Update,
      expectedUpdateNormalizers: Iterable[UpdateNormalizer],
      actualUpdateNormalizers: Iterable[UpdateNormalizer],
      pairwiseUpdateNormalizers: Iterable[PairwiseUpdateNormalizer],
  ): Future[Unit] = {

    val (expectedNormalizedUpdate, actualNormalizedUpdate) = PairwiseUpdateNormalizer.normalize(
      normalize(expectedUpdate, expectedUpdateNormalizers),
      normalize(actualUpdate, actualUpdateNormalizers),
      pairwiseUpdateNormalizers,
    )

    if (expectedNormalizedUpdate != actualNormalizedUpdate) {
      if (expectedNormalizedUpdate == expectedUpdate && actualNormalizedUpdate == actualUpdate) {
        Future.failed(
          new ComparisonFailureException(
            "State update mismatch.",
            "Expected:",
            expectedNormalizedUpdate.toString,
            "Actual:",
            actualNormalizedUpdate.toString,
          )
        )
      } else {
        Future.failed(
          new ComparisonFailureException(
            "State update mismatch.",
            "Expected (before normalization):",
            expectedUpdate.toString,
            "Expected (after normalization):",
            expectedNormalizedUpdate.toString,
            "Actual (before normalization):",
            actualUpdate.toString,
            "Actual (after normalization):",
            actualNormalizedUpdate.toString,
          )
        )
      }
    } else {
      Future.unit
    }
  }
}

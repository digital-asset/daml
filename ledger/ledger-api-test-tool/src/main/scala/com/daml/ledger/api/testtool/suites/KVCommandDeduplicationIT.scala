// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.CommandDeduplicationBase
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.admin.config_management_service.TimeModel
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Command deduplication tests for KV ledgers
  * KV ledgers have both participant side deduplication and committer side deduplication.
  * The committer side deduplication period adds `minSkew` to the participant-side one, so we have to account for that as well.
  * If updating the time model fails then the tests will assume a `minSkew` of 1 second.
  */
final class KVCommandDeduplicationIT(timeoutScaleFactor: Double, ledgerTimeInterval: FiniteDuration)
    extends CommandDeduplicationBase(timeoutScaleFactor, ledgerTimeInterval) {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName)

  override def runGivenDeduplicationWait(
      context: ParticipantTestContext
  )(test: Duration => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    // deduplication duration is increased by minSkew in the committer so we set the skew to a low value for testing
    val minSkew = 1.second.asProtobuf
    runWithUpdatedOrFallbackTimeModel(
      context,
      _.update(_.minSkew := minSkew),
      TimeModel.defaultInstance.update(_.minSkew := minSkew),
    )(timeModel => test(defaultDeduplicationWindowWait.plus(timeModel.getMinSkew.asScala)))
  }

  private def runWithUpdatedOrFallbackTimeModel(
      ledger: ParticipantTestContext,
      timeModelUpdate: TimeModel => TimeModel,
      fallbackTimeModel: => TimeModel,
  )(test: TimeModel => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = ledger
    .getTimeModel()
    .flatMap(timeModel => {
      def restoreTimeModel() = {
        val ledgerTimeModelRestoreResult = for {
          time <- ledger.time()
          _ <- ledger
            .setTimeModel(
              mrt = time.plusSeconds(30),
              generation = timeModel.configurationGeneration + 1,
              newTimeModel = timeModel.getTimeModel,
            )
        } yield {}
        ledgerTimeModelRestoreResult.recover { case NonFatal(exception) =>
          logger.warn("Failed to restore time model for ledger", exception)
          ()
        }
      }
      for {
        time <- ledger.time()
        updatedModel = timeModelUpdate(timeModel.getTimeModel)
        timeModelForTest <- ledger
          .setTimeModel(
            mrt = time.plusSeconds(30),
            generation = timeModel.configurationGeneration,
            newTimeModel = updatedModel,
          )
          .map(_ => updatedModel)
          .recover { case NonFatal(e) =>
            logger.warn("Failed to update time model, running with default", e)
            fallbackTimeModel
          }
        _ <- test(timeModelForTest)
          .transformWith(testResult => restoreTimeModel().transform(_ => testResult))
      } yield {}
    })

}

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
  * The committer side deduplication extends over a greater time interval compared to the participant side deduplication (by `minSkew` more exactly)
  *  so we have to account for that wait period as well
  * If updating the time model fails then the tests will run assuming a `minSkew` of 1 second
  */
final class KVCommandDeduplicationIT(timeoutScaleFactor: Double, ledgerTimeInterval: FiniteDuration)
    extends CommandDeduplicationBase(timeoutScaleFactor, ledgerTimeInterval) {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName)

  override def runGivenDeduplicationWait(
      context: ParticipantTestContext
  )(test: Duration => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    val minSkew = 1.second.asProtobuf
    runWithUpdatedOrExistingTimeModel(
      context,
      _.update(_.minSkew := minSkew),
      TimeModel.defaultInstance.update(_.minSkew := minSkew),
    )(timeModel => test(defaultDeduplicationWindowWait.plus(timeModel.getMinSkew.asScala)))
  }

  private def runWithUpdatedOrExistingTimeModel(
      ledger: ParticipantTestContext,
      timeModelUpdate: TimeModel => TimeModel,
      timeModelIfUpdateFailed: => TimeModel,
  )(test: TimeModel => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    ledger
      .getTimeModel()
      .flatMap(timeModel => {
        def restoreTimeModel() = {
          val ledgerTimeModelRestoreResult = for {
            time <- ledger.time()
            _ <- ledger
              .setTimeModel(
                time.plusSeconds(30),
                timeModel.configurationGeneration + 1,
                timeModel.getTimeModel,
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
              time.plusSeconds(30),
              timeModel.configurationGeneration,
              updatedModel,
            )
            .map(_ => updatedModel)
            .recover { case NonFatal(e) =>
              logger.warn("Failed to update time model, running with default", e)
              timeModelIfUpdateFailed
            }
          _ <- test(timeModelForTest)
            .transformWith(testResult => restoreTimeModel().transform(_ => testResult))
        } yield {}
      })
  }

}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.deduplication.CommandDeduplicationBase
import com.daml.ledger.api.testtool.infrastructure.deduplication.CommandDeduplicationBase.{
  DeduplicationFeatures,
  DelayMechanism,
  StaticTimeDelayMechanism,
  TimeDelayMechanism,
}
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.admin.config_management_service.TimeModel
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Command deduplication tests for KV ledgers
  * KV ledgers have committer side deduplication.
  * The committer side deduplication period adds `minSkew` to the deduplication period, so we have to account for that as well.
  * If updating the time model fails then the tests will assume a `minSkew` of 1 second.
  */
class KVCommandDeduplicationIT(
    timeoutScaleFactor: Double,
    ledgerTimeInterval: FiniteDuration,
    staticTime: Boolean,
) extends CommandDeduplicationBase(timeoutScaleFactor, ledgerTimeInterval, staticTime) {

  private[this] val logger = LoggerFactory.getLogger(getClass.getName)

  override def testNamingPrefix: String = "KVCommandDeduplication"

  override def deduplicationFeatures: CommandDeduplicationBase.DeduplicationFeatures =
    DeduplicationFeatures(
      participantDeduplication = false
    )

  protected override def runWithDeduplicationDelay(
      participants: Seq[ParticipantTestContext]
  )(
      testWithDelayMechanism: DelayMechanism => Future[Unit]
  )(implicit ec: ExecutionContext): Future[Unit] = {
    runWithConfig(participants) { extraWait =>
      val anyParticipant = participants.head
      testWithDelayMechanism(delayMechanism(anyParticipant, extraWait))
    }
  }

  private def delayMechanism(
      ledger: ParticipantTestContext,
      extraWait: Duration,
  )(implicit ec: ExecutionContext) = {
    if (staticTime) {
      new StaticTimeDelayMechanism(ledger, deduplicationDuration, extraWait)
    } else {
      new TimeDelayMechanism(deduplicationDuration, extraWait)
    }
  }

  private def runWithConfig(
      participants: Seq[ParticipantTestContext]
  )(test: FiniteDuration => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    // deduplication duration is adjusted by min skew and max skew when running using pre-execution
    // to account for this we adjust the time model
    val skew = scaledDuration(1.second).asProtobuf
    runWithUpdatedTimeModel(
      participants,
      _.update(_.minSkew := skew, _.maxSkew := skew),
    )(timeModel =>
      test(
        asFiniteDuration(timeModel.getMinSkew.asScala + timeModel.getMaxSkew.asScala)
      )
    )
  }

  private def runWithUpdatedTimeModel(
      participants: Seq[ParticipantTestContext],
      timeModelUpdate: TimeModel => TimeModel,
  )(test: TimeModel => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    val anyParticipant = participants.head
    anyParticipant
      .getTimeModel()
      .flatMap(timeModel => {
        def restoreTimeModel(participant: ParticipantTestContext) = {
          val ledgerTimeModelRestoreResult = for {
            time <- participant.time()
            _ <- participant
              .setTimeModel(
                time.plusSeconds(1),
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
          time <- anyParticipant.time()
          updatedModel = timeModelUpdate(timeModel.getTimeModel)
          (timeModelForTest, participantThatDidTheUpdate) <- tryTimeModelUpdateOnAllParticipants(
            participants,
            _.setTimeModel(
              time.plusSeconds(1),
              timeModel.configurationGeneration,
              updatedModel,
            ).map(_ => updatedModel),
          )
          _ <- test(timeModelForTest)
            .transformWith(testResult =>
              restoreTimeModel(participantThatDidTheUpdate).transform(_ => testResult)
            )
        } yield {}
      })
  }

  /** Try to run the update sequentially on all the participants.
    * The function returns the first success or the last failure of the update operation.
    * Useful for updating the configuration when we don't know which participant can update the config,
    * as only the first one that submitted the initial configuration has the permissions to do so.
    */
  private def tryTimeModelUpdateOnAllParticipants(
      participants: Seq[ParticipantTestContext],
      timeModelUpdate: ParticipantTestContext => Future[TimeModel],
  )(implicit ec: ExecutionContext): Future[(TimeModel, ParticipantTestContext)] = {
    participants.foldLeft(
      Future.failed[(TimeModel, ParticipantTestContext)](
        new IllegalStateException("No participant")
      )
    ) { (result, participant) =>
      result.recoverWith { case NonFatal(_) =>
        timeModelUpdate(participant).map(_ -> participant)
      }
    }
  }
}

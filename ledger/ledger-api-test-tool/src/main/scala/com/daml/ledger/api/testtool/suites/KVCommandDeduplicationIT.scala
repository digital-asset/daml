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

  override def testNamingPrefix: String = "KVCommandDeduplication"

  override def runGivenDeduplicationWait(
      participants: Seq[ParticipantTestContext]
  )(test: Duration => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    // deduplication duration is increased by minSkew in the committer so we set the skew to a low value for testing
    val minSkew = 1.second.asProtobuf
    runWithUpdatedTimeModel(
      participants,
      _.update(_.minSkew := minSkew),
    )(timeModel => test(defaultDeduplicationWindowWait.plus(timeModel.getMinSkew.asScala)))
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
          time <- anyParticipant.time()
          updatedModel = timeModelUpdate(timeModel.getTimeModel)
          (timeModelForTest, participantThatDidTheUpdate) <- tryUpdateOnAllParticipants(
            participants,
            _.setTimeModel(
              time.plusSeconds(30),
              timeModel.configurationGeneration,
              updatedModel,
            )
              .map(_ => updatedModel),
          )
          _ <- test(timeModelForTest)
            .transformWith(testResult =>
              restoreTimeModel(participantThatDidTheUpdate).transform(_ => testResult)
            )
        } yield {}
      })
  }

  /** Try to run the [[update]] sequentially on all the participants.
    * The function returns the first success or the last failure of the update operation.
    * Useful for updating the configuration when we don't know which participant can update the config, as only one has the permissions to do so.
    */
  private def tryUpdateOnAllParticipants(
      participants: Seq[ParticipantTestContext],
      update: ParticipantTestContext => Future[TimeModel],
  )(implicit ec: ExecutionContext): Future[(TimeModel, ParticipantTestContext)] = {
    participants.foldLeft(
      Future.failed[(TimeModel, ParticipantTestContext)](new IllegalStateException("No success"))
    ) { (result, participant) =>
      result.recoverWith { case NonFatal(_) =>
        update(participant).map(_ -> participant)
      }
    }
  }

}

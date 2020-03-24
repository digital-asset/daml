// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Clock
import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntryId, DamlSubmissionBatch}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar._
import org.scalatest.{AsyncWordSpec, Inside, Matchers}

import scala.concurrent.Future

class BatchValidatorSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with AkkaBeforeAndAfterAll {
  private def allocateRandomLogEntryId(): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      .build()

  private class FakeLedgerState(mockOps: LedgerOps) extends LedgerState {
    override def inTransaction[T](body: LedgerOps => Future[T]): Future[T] =
      body(mockOps)
  }

  private def newRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  private def aParticipantId(): ParticipantId = ParticipantId.assertFromString("aParticipantId")

  "validate" should {

    "return invalid submission for invalid envelope in batch" in {

      val mockStateOps = mock[LedgerOps]
      val validator = new BatchValidator(
        BatchValidationParameters.default,
        () => allocateRandomLogEntryId,
        new FakeLedgerState(mockStateOps)
      )

      val batchBuilder =
        DamlSubmissionBatch.newBuilder

      batchBuilder.addSubmissionsBuilder
        .setCorrelationId("aCorrelationId")
        .setSubmission(ByteString.copyFromUtf8("bad data"))

      validator
        .validateAndCommit(
          newRecordTime().toInstant,
          aParticipantId(),
          Envelope.enclose(batchBuilder.build))
        .failed
        .map { result =>
          result shouldBe a[ValidationFailed]
        }
    }

    // FIXME(JM): Write more tests stupid.
  }
}

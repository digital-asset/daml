// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.{Clock, Duration}
import java.util.UUID

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.ledger.participant.state.v2.{SubmissionResult, SubmitterInfo, TransactionMeta}
import com.daml.ledger.validator.{
  DefaultStateKeySerializationStrategy,
  StateKeySerializationStrategy,
}
import com.daml.lf.crypto
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.metrics.Metrics
import com.daml.telemetry.{NoOpTelemetryContext, TelemetryContext}
import org.mockito.captor.{ArgCaptor, Captor}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class KeyValueParticipantStateWriterSpec
    extends AnyWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  import KeyValueParticipantStateWriterSpec._

  "participant state writer" should {
    "submit a transaction" in {
      val transactionCaptor = ArgCaptor[Raw.Envelope]
      val correlationIdCaptor = ArgCaptor[String]
      val metadataCaptor = ArgCaptor[CommitMetadata]
      val writer = createWriter(transactionCaptor, metadataCaptor, correlationIdCaptor)
      val instance = new KeyValueParticipantStateWriter(writer, newMetrics())
      val recordTime = newRecordTime()
      val expectedCorrelationId = "correlation ID"

      instance.submitTransaction(
        submitterInfo(aParty, expectedCorrelationId),
        transactionMeta(recordTime),
        TransactionBuilder.EmptySubmitted,
        anInterpretationCost,
      )

      verify(writer, times(1)).commit(any[String], any[Raw.Envelope], any[CommitMetadata])(
        any[TelemetryContext]
      )
      verifyEnvelope(transactionCaptor.value)(_.hasTransactionEntry)
      correlationIdCaptor.value should be(expectedCorrelationId)
      val actualCommitMetadata = metadataCaptor.value
      actualCommitMetadata.estimatedInterpretationCost shouldBe defined
      actualCommitMetadata.inputKeys(aSerializationStrategy) should not be empty
      actualCommitMetadata.outputKeys(aSerializationStrategy) should not be empty
    }

    "upload a package" in {
      val packageUploadCaptor = ArgCaptor[Raw.Envelope]
      val metadataCaptor = ArgCaptor[CommitMetadata]
      val writer = createWriter(packageUploadCaptor, metadataCaptor)
      val instance = new KeyValueParticipantStateWriter(writer, newMetrics())

      instance.uploadPackages(aSubmissionId, List.empty, sourceDescription = None)

      verify(writer, times(1)).commit(any[String], any[Raw.Envelope], any[CommitMetadata])(
        any[TelemetryContext]
      )
      verifyEnvelope(packageUploadCaptor.value)(_.hasPackageUploadEntry)
      val actualCommitMetadata = metadataCaptor.value
      actualCommitMetadata.inputKeys(aSerializationStrategy) should not be empty
      actualCommitMetadata.outputKeys(aSerializationStrategy) should not be empty
    }

    "submit a configuration" in {
      val configurationCaptor = ArgCaptor[Raw.Envelope]
      val metadataCaptor = ArgCaptor[CommitMetadata]
      val writer = createWriter(configurationCaptor, metadataCaptor)
      val instance = new KeyValueParticipantStateWriter(writer, newMetrics())

      instance.submitConfiguration(newRecordTime().addMicros(10000), aSubmissionId, aConfiguration)

      verify(writer, times(1)).commit(any[String], any[Raw.Envelope], any[CommitMetadata])(
        any[TelemetryContext]
      )
      verifyEnvelope(configurationCaptor.value)(_.hasConfigurationSubmission)
      val actualCommitMetadata = metadataCaptor.value
      actualCommitMetadata.inputKeys(aSerializationStrategy) should not be empty
      actualCommitMetadata.outputKeys(aSerializationStrategy) should not be empty
    }

    "allocate a party without hint" in {
      val partyAllocationCaptor = ArgCaptor[Raw.Envelope]
      val metadataCaptor = ArgCaptor[CommitMetadata]
      val writer = createWriter(partyAllocationCaptor, metadataCaptor)
      val instance = new KeyValueParticipantStateWriter(writer, newMetrics())

      instance.allocateParty(hint = None, displayName = None, aSubmissionId)

      verify(writer, times(1)).commit(any[String], any[Raw.Envelope], any[CommitMetadata])(
        any[TelemetryContext]
      )
      verifyEnvelope(partyAllocationCaptor.value)(_.hasPartyAllocationEntry)
      val actualCommitMetadata = metadataCaptor.value
      actualCommitMetadata.inputKeys(aSerializationStrategy) should not be empty
      actualCommitMetadata.outputKeys(aSerializationStrategy) should not be empty
    }
  }

  private def verifyEnvelope(
      written: Raw.Envelope
  )(assertion: DamlSubmission => Boolean): Assertion =
    Envelope.openSubmission(written) match {
      case Right(value) => assert(assertion(value) === true)
      case _ => fail()
    }
}

object KeyValueParticipantStateWriterSpec {

  import MockitoSugar._

  private implicit val telemetryContext: TelemetryContext = NoOpTelemetryContext

  private val aParty = Ref.Party.assertFromString("aParty")

  private val aSubmissionId: Ref.SubmissionId =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)

  private val aConfiguration: Configuration = Configuration(
    generation = 1,
    timeModel = LedgerTimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofDays(1),
  )

  private val anInterpretationCost = 123L

  private val aSerializationStrategy: StateKeySerializationStrategy =
    DefaultStateKeySerializationStrategy

  private def createWriter(
      envelopeCaptor: Captor[Raw.Envelope],
      metadataCaptor: Captor[CommitMetadata],
      correlationIdCaptor: Captor[String] = ArgCaptor[String],
  ): LedgerWriter = {
    val writer = mock[LedgerWriter]
    when(
      writer.commit(correlationIdCaptor.capture, envelopeCaptor.capture, metadataCaptor.capture)(
        ArgCaptor[TelemetryContext]
      )
    )
      .thenReturn(Future.successful(SubmissionResult.Acknowledged))
    when(writer.participantId).thenReturn(Ref.ParticipantId.assertFromString("test-participant"))
    writer
  }

  private def submitterInfo(party: Ref.Party, submissionId: String) =
    SubmitterInfo(
      actAs = List(party),
      applicationId = Ref.LedgerString.assertFromString("tests"),
      commandId = Ref.LedgerString.assertFromString("someCommandId"),
      deduplicationPeriod = DeduplicationPeriod.DeduplicationDuration(Duration.ofDays(1)),
      submissionId = Some(Ref.SubmissionId.assertFromString(submissionId)),
      ledgerConfiguration =
        Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofSeconds(1)),
    )

  private def transactionMeta(let: Timestamp) = TransactionMeta(
    ledgerEffectiveTime = let,
    workflowId = Some(Ref.LedgerString.assertFromString("tests")),
    submissionTime = let.addMicros(1000),
    submissionSeed = crypto.Hash.assertFromString(
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    ),
    optUsedPackages = Some(Set.empty),
    optNodeSeeds = None,
    optByKeyNodes = None,
  )

  private def newMetrics(): Metrics = new Metrics(new MetricRegistry)

  private def newRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}

// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.{Clock, Duration}
import java.util.UUID

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.kvutils.MockitoHelpers.captor
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateWriterSpec._
import com.daml.ledger.participant.state.kvutils.{Bytes, Envelope}
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1._
import com.daml.lf.crypto
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.{GenTransaction, Transaction}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar._
import org.scalatest.{Assertion, WordSpec}

import scala.collection.immutable.HashMap
import scala.concurrent.Future

class KeyValueParticipantStateWriterSpec extends WordSpec {
  "participant state writer" should {
    "submit a transaction" in {
      val transactionCaptor = captor[Bytes]
      val writer = createWriter(transactionCaptor)
      val instance = new KeyValueParticipantStateWriter(writer, new MetricRegistry)
      val recordTime = newRecordTime()

      instance.submitTransaction(
        submitterInfo(recordTime, aParty),
        transactionMeta(recordTime),
        anEmptyTransaction)
      verify(writer, times(1)).commit(anyString(), any[Bytes]())
      verifyEnvelope(transactionCaptor.getValue)(_.hasTransactionEntry)
    }

    "upload a package" in {
      val packageUploadCaptor = captor[Bytes]
      val writer = createWriter(packageUploadCaptor)
      val instance = new KeyValueParticipantStateWriter(writer, new MetricRegistry)

      instance.uploadPackages(aSubmissionId, List.empty, sourceDescription = None)
      verify(writer, times(1)).commit(anyString(), any[Bytes]())
      verifyEnvelope(packageUploadCaptor.getValue)(_.hasPackageUploadEntry)
    }

    "submit a configuration" in {
      val configurationCaptor = captor[Bytes]
      val writer = createWriter(configurationCaptor)
      val instance = new KeyValueParticipantStateWriter(writer, new MetricRegistry)

      instance.submitConfiguration(newRecordTime().addMicros(10000), aSubmissionId, aConfiguration)
      verify(writer, times(1)).commit(anyString(), any[Bytes]())
      verifyEnvelope(configurationCaptor.getValue)(_.hasConfigurationSubmission)
    }

    "allocate a party without hint" in {
      val partyAllocationCaptor = captor[Bytes]
      val writer = createWriter(partyAllocationCaptor)
      val instance = new KeyValueParticipantStateWriter(writer, new MetricRegistry)

      instance.allocateParty(hint = None, displayName = None, aSubmissionId)
      verify(writer, times(1)).commit(anyString(), any[Bytes]())
      verifyEnvelope(partyAllocationCaptor.getValue)(_.hasPartyAllocationEntry)
    }
  }

  private def verifyEnvelope(written: Bytes)(assertion: DamlSubmission => Boolean): Assertion =
    Envelope.openSubmission(written) match {
      case Right(value) => assert(assertion(value) === true)
      case _ => fail()
    }
}

object KeyValueParticipantStateWriterSpec {

  private val aParty = Ref.Party.assertFromString("aParty")

  private val anEmptyTransaction: Transaction.AbsTransaction =
    GenTransaction(HashMap.empty, ImmArray.empty)

  private val aSubmissionId: SubmissionId =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)

  private val aConfiguration: Configuration = Configuration(
    generation = 1,
    timeModel = TimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofDays(1),
  )

  private def createWriter(captor: ArgumentCaptor[Bytes]): LedgerWriter = {
    val writer = mock[LedgerWriter]
    when(writer.commit(anyString(), captor.capture()))
      .thenReturn(Future.successful(SubmissionResult.Acknowledged))
    when(writer.participantId).thenReturn(v1.ParticipantId.assertFromString("test-participant"))
    writer
  }

  private def submitterInfo(rt: Timestamp, party: Ref.Party) = SubmitterInfo(
    submitter = party,
    applicationId = Ref.LedgerString.assertFromString("tests"),
    commandId = Ref.LedgerString.assertFromString("X"),
    deduplicateUntil = rt.addMicros(Duration.ofDays(1).toNanos / 1000).toInstant,
  )

  private def transactionMeta(let: Timestamp) = TransactionMeta(
    ledgerEffectiveTime = let,
    workflowId = Some(Ref.LedgerString.assertFromString("tests")),
    submissionTime = let.addMicros(1000),
    submissionSeed = Some(
      crypto.Hash.assertFromString(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")),
    optUsedPackages = Some(Set.empty)
  )

  private def newRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}

// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.{Clock, Duration}
import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{ImmArray, InsertOrdSet, Ref}
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Assertion, WordSpec}

import scala.collection.immutable.HashMap
import scala.concurrent.{ExecutionContext, Future}

class KeyValueParticipantStateWriterSpec extends WordSpec with MockitoSugar {
  implicit val executionContext: ExecutionContext = ExecutionContext.global

  "participant state writer" should {
    "submit a transaction" in {
      val transactionCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
      val writer = createWriter(Some(transactionCaptor))
      val instance = new KeyValueParticipantStateWriter(writer)
      val recordTime = newRecordTime()

      instance.submitTransaction(
        submitterInfo(recordTime, aParty),
        transactionMeta(recordTime),
        anEmptyTransaction)
      verify(writer, times(1)).commit(anyString(), any[Array[Byte]]())
      verifyEnvelope(transactionCaptor.getValue)(_.hasTransactionEntry)
    }

    "upload a package" in {
      val packageUploadCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
      val writer = createWriter(Some(packageUploadCaptor))
      val instance = new KeyValueParticipantStateWriter(writer)

      instance.uploadPackages(aSubmissionId, List.empty, sourceDescription = None)
      verify(writer, times(1)).commit(anyString(), any[Array[Byte]]())
      verifyEnvelope(packageUploadCaptor.getValue)(_.hasPackageUploadEntry)
    }

    "submit a configuration" in {
      val configurationCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
      val writer = createWriter(Some(configurationCaptor))
      val instance = new KeyValueParticipantStateWriter(writer)

      instance.submitConfiguration(newRecordTime().addMicros(10000), aSubmissionId, aConfiguration)
      verify(writer, times(1)).commit(anyString(), any[Array[Byte]]())
      verifyEnvelope(configurationCaptor.getValue)(_.hasConfigurationSubmission)
    }

    "allocate a party without hint" in {
      val partyAllocationCaptor = ArgumentCaptor.forClass(classOf[Array[Byte]])
      val writer = createWriter(Some(partyAllocationCaptor))
      val instance = new KeyValueParticipantStateWriter(writer)

      instance.allocateParty(hint = None, displayName = None, aSubmissionId)
      verify(writer, times(1)).commit(anyString(), any[Array[Byte]]())
      verifyEnvelope(partyAllocationCaptor.getValue)(_.hasPartyAllocationEntry)
    }
  }

  private def verifyEnvelope(written: Array[Byte])(
      assertion: DamlSubmission => Boolean): Assertion =
    Envelope.openSubmission(written) match {
      case Right(value) => assert(assertion(value) === true)
      case _ => fail()
    }

  private val aParty = Ref.Party.assertFromString("aParty")

  private val anEmptyTransaction: Transaction.AbsTransaction =
    GenTransaction(HashMap.empty, ImmArray.empty, Some(InsertOrdSet.empty))

  private val aSubmissionId: SubmissionId =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)

  private val aConfiguration: Configuration = Configuration(1, TimeModel.reasonableDefault)

  private def createWriter(captor: Option[ArgumentCaptor[Array[Byte]]] = None): LedgerWriter = {
    val writer = mock[LedgerWriter]
    when(writer.commit(anyString(), captor.map(_.capture()).getOrElse(any[Array[Byte]]())))
      .thenReturn(Future.successful(SubmissionResult.Acknowledged))
    when(writer.participantId).thenReturn(v1.ParticipantId.assertFromString("test-participant"))
    writer
  }

  private def submitterInfo(rt: Timestamp, party: Ref.Party) = SubmitterInfo(
    submitter = party,
    applicationId = Ref.LedgerString.assertFromString("tests"),
    commandId = Ref.LedgerString.assertFromString("X"),
    maxRecordTime = rt.addMicros(Duration.ofSeconds(10).toNanos / 1000)
  )

  private def transactionMeta(let: Timestamp) = TransactionMeta(
    ledgerEffectiveTime = let,
    workflowId = Some(Ref.LedgerString.assertFromString("tests")),
    submissionSeed = Some(
      crypto.Hash.assertFromString(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"))
  )

  private def newRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}

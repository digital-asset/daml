// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.CommandRejected.FinalReason
import com.digitalasset.canton.ledger.participant.state.Update.{
  RepairTransactionAccepted,
  SequencedCommandRejected,
  SequencedTransactionAccepted,
  UnSequencedCommandRejected,
}
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, TransactionMeta}
import com.digitalasset.canton.logging.{NamedLogging, SuppressingLogger}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.digitalasset.daml.lf.transaction.CommittedTransaction
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import io.grpc.Status
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class PostPublishDataSpec extends AnyFlatSpec with Matchers with NamedLogging {
  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)

  private val domainId = DomainId.tryFromString("x::domain1")
  private val party = Ref.Party.assertFromString("party")
  private val applicationId = Ref.ApplicationId.assertFromString("applicationid1")
  private val cantonTime1 = CantonTimestamp.now()
  private val cantonTime2 = CantonTimestamp.now()
  private val commandId = Ref.CommandId.assertFromString(UUID.randomUUID().toString)
  private val offset = Offset.fromLong(15)
  private val submissionId = Some(Ref.SubmissionId.assertFromString(UUID.randomUUID().toString))
  private val updateId = Ref.TransactionId.fromLong(15000)
  private val someHash =
    crypto.Hash.assertFromString("01cf85cfeb36d628ca2e6f583fa2331be029b6b28e877e1008fb3f862306c086")
  private val transactionMeta = TransactionMeta(
    ledgerEffectiveTime = Time.Timestamp.assertFromLong(2),
    workflowId = None,
    submissionTime = Time.Timestamp.assertFromLong(3),
    submissionSeed = someHash,
    optUsedPackages = None,
    optNodeSeeds = None,
    optByKeyNodes = None,
  )
  private val status =
    com.google.rpc.status.Status.of(Status.Code.ABORTED.value(), "test reason", Seq.empty)
  private val messageUuid = UUID.randomUUID()

  behavior of "from"

  it should "populate post PostPublishData correctly for TransactionAccepted" in {
    PostPublishData.from(
      update = SequencedTransactionAccepted(
        completionInfoO = Some(
          CompletionInfo(
            actAs = List(party),
            applicationId = applicationId,
            commandId = commandId,
            optDeduplicationPeriod = None,
            submissionId = submissionId,
          )
        ),
        transactionMeta = transactionMeta,
        transaction = CommittedTransaction(TransactionBuilder.Empty),
        updateId = updateId,
        hostedWitnesses = Nil,
        contractMetadata = Map.empty,
        domainId = domainId,
        requestCounter = RequestCounter(65),
        sequencerCounter = SequencerCounter(11),
        recordTime = cantonTime2,
      )(TraceContext.empty),
      offset = offset,
      publicationTime = cantonTime1,
    ) shouldBe Some(
      PostPublishData(
        submissionDomainId = domainId,
        publishSource = PublishSource.Sequencer(
          requestSequencerCounter = SequencerCounter(11),
          sequencerTimestamp = cantonTime2,
        ),
        applicationId = applicationId,
        commandId = commandId,
        actAs = Set(party),
        offset = offset,
        publicationTime = cantonTime1,
        submissionId = submissionId,
        accepted = true,
        traceContext = TraceContext.empty,
      )
    )
  }

  it should "not populate post PostPublishData correctly for TransactionAccepted without completion info" in {
    PostPublishData.from(
      update = SequencedTransactionAccepted(
        completionInfoO = None,
        transactionMeta = transactionMeta,
        transaction = CommittedTransaction(TransactionBuilder.Empty),
        updateId = updateId,
        hostedWitnesses = Nil,
        contractMetadata = Map.empty,
        domainId = domainId,
        requestCounter = RequestCounter(65),
        sequencerCounter = SequencerCounter(11),
        recordTime = cantonTime2,
      )(TraceContext.empty),
      offset = offset,
      publicationTime = cantonTime1,
    ) shouldBe None
  }

  it should "populate no post PostPublishData for TransactionAccepted without request sequencer counter" in {
    PostPublishData.from(
      update = RepairTransactionAccepted(
        transactionMeta = transactionMeta,
        transaction = CommittedTransaction(TransactionBuilder.Empty),
        updateId = updateId,
        hostedWitnesses = Nil,
        contractMetadata = Map.empty,
        domainId = domainId,
        requestCounter = RequestCounter(65),
        recordTime = cantonTime2,
      )(TraceContext.empty),
      offset = offset,
      publicationTime = cantonTime1,
    ) shouldBe None
  }

  it should "populate post PostPublishData correctly for CommandRejected for sequenced" in {
    PostPublishData.from(
      update = SequencedCommandRejected(
        completionInfo = CompletionInfo(
          actAs = List(party),
          applicationId = applicationId,
          commandId = commandId,
          optDeduplicationPeriod = None,
          submissionId = submissionId,
        ),
        reasonTemplate = FinalReason(status),
        domainId = domainId,
        requestCounter = RequestCounter(65),
        sequencerCounter = SequencerCounter(11),
        recordTime = cantonTime2,
      )(TraceContext.empty),
      offset = offset,
      publicationTime = cantonTime1,
    ) shouldBe Some(
      PostPublishData(
        submissionDomainId = domainId,
        publishSource = PublishSource.Sequencer(
          requestSequencerCounter = SequencerCounter(11),
          sequencerTimestamp = cantonTime2,
        ),
        applicationId = applicationId,
        commandId = commandId,
        actAs = Set(party),
        offset = offset,
        publicationTime = cantonTime1,
        submissionId = submissionId,
        accepted = false,
        traceContext = TraceContext.empty,
      )
    )
  }

  it should "populate post PostPublishData correctly for CommandRejected for non-sequenced" in {
    PostPublishData.from(
      update = UnSequencedCommandRejected(
        completionInfo = CompletionInfo(
          actAs = List(party),
          applicationId = applicationId,
          commandId = commandId,
          optDeduplicationPeriod = None,
          submissionId = submissionId,
        ),
        reasonTemplate = FinalReason(status),
        domainId = domainId,
        recordTime = cantonTime2,
        messageUuid = messageUuid,
      )(TraceContext.empty),
      offset = offset,
      publicationTime = cantonTime1,
    ) shouldBe Some(
      PostPublishData(
        submissionDomainId = domainId,
        publishSource = PublishSource.Local(messageUuid),
        applicationId = applicationId,
        commandId = commandId,
        actAs = Set(party),
        offset = offset,
        publicationTime = cantonTime1,
        submissionId = submissionId,
        accepted = false,
        traceContext = TraceContext.empty,
      )
    )
  }

}

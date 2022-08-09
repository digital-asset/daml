// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2.{CompletionInfo, TransactionMeta, Update}
import com.daml.lf.crypto
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.metrics.Metrics
import com.daml.platform.index.InMemoryStateUpdater.PrepareResult
import com.daml.platform.index.InMemoryStateUpdaterSpec.{
  Scope,
  anotherMetadataChangedUpdate,
  metadataChangedUpdate,
  update1,
  update3,
  update4,
  update5,
}
import com.daml.platform.indexer.ha.EndlessReadService.configuration
import com.daml.platform.store.packagemeta.PackageMetadataView.PackageMetadata
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.chaining._
import scala.collection.mutable.ArrayBuffer

class InMemoryStateUpdaterSpec extends AnyFlatSpec with Matchers with AkkaBeforeAndAfterAll {

  behavior of classOf[InMemoryStateUpdater].getSimpleName

  "flow" should "correctly process updates" in new Scope {
    runFlow(
      Seq(
        Vector(update1, metadataChangedUpdate) -> 1L,
        Vector(update3, update4) -> 3L,
        Vector(update5) -> 4L,
      )
    )
    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(1L),
      result(3L),
      result(4L),
    )
  }

  "flow" should "not process empty input batches" in new Scope {
    runFlow(
      Seq(
        // Empty input batch should have not effect
        Vector.empty -> 1L,
        Vector(update3) -> 3L,
        // Results in empty batch after processing
        // Should still have effect on ledger end updates
        Vector(anotherMetadataChangedUpdate) -> 3L,
        Vector(update5) -> 4L,
      )
    )

    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(3L),
      result(3L),
      result(4L),
    )
  }
}

object InMemoryStateUpdaterSpec {
  trait Scope extends Matchers with ScalaFutures with IntegrationPatience {

    val cacheUpdates = ArrayBuffer.empty[PrepareResult]
    val cachesUpdateCaptor =
      (v: PrepareResult) => cacheUpdates.addOne(v).pipe(_ => ())

    def result(lastEventSequentialId: Long) =
      PrepareResult(Vector.empty, offset(1L), lastEventSequentialId, PackageMetadata())

    val inMemoryStateUpdater = new InMemoryStateUpdater(
      2,
      scala.concurrent.ExecutionContext.global,
      scala.concurrent.ExecutionContext.global,
      new Metrics(new MetricRegistry),
    )(
      prepare = (_, lastEventSequentialId) => result(lastEventSequentialId),
      update = cachesUpdateCaptor,
    )

    def runFlow(input: Seq[(Vector[(Offset, Update)], Long)])(implicit mat: Materializer): Done =
      Source(input)
        .via(inMemoryStateUpdater.flow)
        .runWith(Sink.ignore)
        .futureValue
  }

  private val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("EndlessReadServiceParticipant")

  private val txId1 = Ref.TransactionId.assertFromString("tx1")
  private val txId2 = Ref.TransactionId.assertFromString("tx2")

  private val someSubmissionId: Ref.SubmissionId =
    Ref.SubmissionId.assertFromString("some submission id")
  private val workflowId: Ref.WorkflowId = Ref.WorkflowId.assertFromString("Workflow")
  private val someTransactionMeta: TransactionMeta = TransactionMeta(
    ledgerEffectiveTime = Timestamp.Epoch,
    workflowId = Some(workflowId),
    submissionTime = Timestamp.Epoch,
    submissionSeed = crypto.Hash.hashPrivateKey("SomeTxMeta"),
    optUsedPackages = None,
    optNodeSeeds = None,
    optByKeyNodes = None,
  )

  private val update1 = offset(1L) -> Update.TransactionAccepted(
    optCompletionInfo = None,
    transactionMeta = someTransactionMeta,
    transaction = CommittedTransaction(TransactionBuilder.Empty),
    transactionId = txId1,
    recordTime = Timestamp.Epoch,
    divulgedContracts = List.empty,
    blindingInfo = None,
    contractMetadata = Map.empty,
  )
  private val metadataChangedUpdate = offset(2L) -> Update.ConfigurationChanged(
    Timestamp.Epoch,
    someSubmissionId,
    participantId,
    configuration,
  )
  private val update3 = offset(3L) -> Update.TransactionAccepted(
    optCompletionInfo = None,
    transactionMeta = someTransactionMeta,
    transaction = CommittedTransaction(TransactionBuilder.Empty),
    transactionId = txId2,
    recordTime = Timestamp.Epoch,
    divulgedContracts = List.empty,
    blindingInfo = None,
    contractMetadata = Map.empty,
  )
  private val update4 = offset(4L) -> Update.CommandRejected(
    recordTime = Time.Timestamp.assertFromLong(1337L),
    completionInfo = CompletionInfo(
      actAs = List.empty,
      applicationId = Ref.ApplicationId.assertFromString("some-app-id"),
      commandId = Ref.CommandId.assertFromString("cmdId"),
      optDeduplicationPeriod = None,
      submissionId = None,
      statistics = None,
    ),
    reasonTemplate = FinalReason(new Status()),
  )
  private val archive = DamlLf.Archive.newBuilder
    .setHash("00001")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 1"))
    .build

  private val update5 = offset(5L) -> Update.PublicPackageUpload(
    archives = List(archive),
    sourceDescription = None,
    recordTime = Timestamp.Epoch,
    submissionId = None,
  )

  private val anotherMetadataChangedUpdate =
    offset(5L) -> metadataChangedUpdate._2.copy(recordTime = Time.Timestamp.assertFromLong(1337L))

  private def offset(idx: Long): Offset = {
    val base = BigInt(1) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}

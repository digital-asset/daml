// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.Done
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2.{CompletionInfo, TransactionMeta, Update}
import com.daml.lf.crypto
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.index.InMemoryStateUpdater.PrepareResult
import com.daml.platform.index.InMemoryStateUpdaterSpec.{
  Scope,
  anotherMetadataChangedUpdate,
  metadataChangedUpdate,
  offset,
  update1,
  update3,
  update4,
  update5,
  update6,
}
import com.daml.platform.indexer.ha.EndlessReadService.configuration
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.packagemeta.PackageMetadataView.PackageMetadata
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.chaining._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration

class InMemoryStateUpdaterSpec extends AnyFlatSpec with Matchers with AkkaBeforeAndAfterAll {

  "flow" should "correctly process updates in order" in new Scope {
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
        Vector(anotherMetadataChangedUpdate) -> 3L,
        Vector(update5) -> 4L,
      )
    )

    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(3L),
      result(3L), // Results in empty batch after processing
      result(4L), // Should still have effect on ledger end updates
    )
  }

  "prepare" should "throw exception for an empty vector" in new Scope {
    an[NoSuchElementException] should be thrownBy {
      InMemoryStateUpdater.prepare(emptyArchiveToMetadata)(Vector.empty, 0L)
    }
  }

  "prepare" should "prepare a batch of a single update" in new Scope {
    InMemoryStateUpdater.prepare(emptyArchiveToMetadata)(
      Vector(update1),
      0L,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(1L),
      0L,
      PackageMetadata(),
    )
  }

  "prepare" should "set last offset and eventSequentialId to last element" in new Scope {
    InMemoryStateUpdater.prepare(emptyArchiveToMetadata)(
      Vector(update1, metadataChangedUpdate),
      6L,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(2L),
      6L,
      PackageMetadata(),
    )
  }

  "prepare" should "append package metadata" in new Scope {
    def metadata: DamlLf.Archive => PackageMetadata = {
      case archive if archive.getHash == "00001" => PackageMetadata(templates = Set(templateId))
      case archive if archive.getHash == "00002" => PackageMetadata(templates = Set(templateId2))
    }

    InMemoryStateUpdater.prepare(metadata)(
      Vector(update5, update6),
      0L,
    ) shouldBe PrepareResult(
      Vector(),
      offset(6L),
      0L,
      PackageMetadata(templates = Set(templateId, templateId2)),
    )
  }
}

object InMemoryStateUpdaterSpec {
  trait Scope extends Matchers with ScalaFutures with IntegrationPatience {

    val templateId = Identifier.assertFromString("noPkgId:Mod:I")
    val templateId2 = Identifier.assertFromString("noPkgId:Mod:I2")

    val emptyArchiveToMetadata: DamlLf.Archive => PackageMetadata = _ => PackageMetadata()
    val cacheUpdates = ArrayBuffer.empty[PrepareResult]
    val cachesUpdateCaptor =
      (v: PrepareResult) => cacheUpdates.addOne(v).pipe(_ => ())

    def result(lastEventSequentialId: Long) =
      PrepareResult(Vector.empty, offset(1L), lastEventSequentialId, PackageMetadata())

    val inMemoryStateUpdater = InMemoryStateUpdaterFlow(
      2,
      scala.concurrent.ExecutionContext.global,
      scala.concurrent.ExecutionContext.global,
      FiniteDuration(10, "seconds"),
      Metrics.ForTesting,
    )(
      prepare = (_, lastEventSequentialId) => result(lastEventSequentialId),
      update = cachesUpdateCaptor,
    )(LoggingContext.empty)

    val txLogUpdate1 = TransactionLogUpdate.TransactionAccepted(
      transactionId = "tx1",
      commandId = "",
      workflowId = workflowId,
      effectiveAt = Timestamp.Epoch,
      offset = offset(1L),
      events = Vector(),
      completionDetails = None,
    )

    def runFlow(input: Seq[(Vector[(Offset, Update)], Long)])(implicit mat: Materializer): Done =
      Source(input)
        .via(inMemoryStateUpdater)
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

  private val archive2 = DamlLf.Archive.newBuilder
    .setHash("00002")
    .setHashFunction(DamlLf.HashFunction.SHA256)
    .setPayload(ByteString.copyFromUtf8("payload 2"))
    .build

  private val update5 = offset(5L) -> Update.PublicPackageUpload(
    archives = List(archive),
    sourceDescription = None,
    recordTime = Timestamp.Epoch,
    submissionId = None,
  )

  private val update6 = offset(6L) -> Update.PublicPackageUpload(
    archives = List(archive2),
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

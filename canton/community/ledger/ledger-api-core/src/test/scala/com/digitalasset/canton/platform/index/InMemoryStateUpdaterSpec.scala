// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import cats.syntax.bifunctor.toBifunctorOps
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.daml.lf.crypto
import com.daml.lf.data.Ref.Identifier
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Bytes, Ref, Time}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.daml.lf.transaction.{CommittedTransaction, NodeId}
import com.daml.lf.value.Value
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.digitalasset.canton.ledger.participant.state.v2.*
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.index.InMemoryStateUpdater.PrepareResult
import com.digitalasset.canton.platform.index.InMemoryStateUpdaterSpec.*
import com.digitalasset.canton.platform.indexer.ha.EndlessReadService.configuration
import com.digitalasset.canton.platform.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.CreatedEvent
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.{
  TemplateIdWithPriority,
  TemplatesForQualifiedName,
}
import com.digitalasset.canton.platform.store.packagemeta.{PackageMetadata, PackageMetadataView}
import com.digitalasset.canton.platform.{DispatcherState, InMemoryState}
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutorServiceGeneric, TestEssentials}
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.mockito.{InOrder, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.util.chaining.*

class InMemoryStateUpdaterSpec
    extends AnyFlatSpec
    with Matchers
    with PekkoBeforeAndAfterAll
    with MockitoSugar
    with BaseTest {

  "flow" should "correctly process updates in order" in new Scope {
    runFlow(
      Seq(
        Vector(update1, metadataChangedUpdate) -> 1L,
        Vector(update3, update4) -> 3L,
        Vector(update5, update7, update8) -> 6L,
      )
    )
    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(1L),
      result(3L),
      result(6L),
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
      InMemoryStateUpdater.prepare(emptyArchiveToMetadata, false)(Vector.empty, 0L)
    }
  }

  "prepare" should "prepare a batch of a single update" in new Scope {
    InMemoryStateUpdater.prepare(emptyArchiveToMetadata, false)(
      Vector(update1),
      0L,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(1L),
      0L,
      update1._2.traceContext,
      PackageMetadata(),
    )
  }

  "prepare" should "prepare a batch without reassignments if multi domain is disabled" in new Scope {
    InMemoryStateUpdater.prepare(emptyArchiveToMetadata, false)(
      Vector(update1, update7, update8),
      0L,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(8L),
      0L,
      update1._2.traceContext,
      PackageMetadata(),
    )
  }

  "prepare" should "prepare a batch with reassignments if multi domain is enabled" in new Scope {
    InMemoryStateUpdater.prepare(emptyArchiveToMetadata, true)(
      Vector(update1, update7, update8),
      0L,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1, assignLogUpdate, unassignLogUpdate),
      offset(8L),
      0L,
      update1._2.traceContext,
      PackageMetadata(),
    )
  }

  "prepare" should "set last offset and eventSequentialId to last element" in new Scope {
    InMemoryStateUpdater.prepare(emptyArchiveToMetadata, false)(
      Vector(update1, metadataChangedUpdate),
      6L,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(2L),
      6L,
      metadataChangedUpdate._2.traceContext,
      PackageMetadata(),
    )
  }

  "prepare" should "append package metadata" in new Scope {
    def metadata: (DamlLf.Archive, Time.Timestamp) => PackageMetadata = {
      case (archive, _) if archive.getHash == "00001" =>
        PackageMetadata(templates = Map(templatesForQn1))
      case (archive, _) if archive.getHash == "00002" =>
        PackageMetadata(templates = Map(templatesForQn2))
      case _ => fail("unexpected archive hash")
    }

    InMemoryStateUpdater.prepare(metadata, false)(
      Vector(update5, update6),
      0L,
    ) shouldBe PrepareResult(
      Vector(),
      offset(6L),
      0L,
      update6._2.traceContext,
      PackageMetadata(templates = Map(templatesForQn1, templatesForQn2)),
    )
  }

  "update" should "update the in-memory state" in new Scope {
    InMemoryStateUpdater.update(inMemoryState, logger)(prepareResult)

    inOrder.verify(packageMetadataView).update(packageMetadata)
    // TODO(i12283) LLP: Unit test contract state event conversion and cache updating

    inOrder
      .verify(inMemoryFanoutBuffer)
      .push(
        tx_accepted_withCompletionDetails_offset,
        tx_accepted_withCompletionDetails,
      )
    inOrder
      .verify(inMemoryFanoutBuffer)
      .push(tx_accepted_withoutCompletionDetails_offset, tx_accepted_withoutCompletionDetails)
    inOrder.verify(inMemoryFanoutBuffer).push(tx_rejected_offset, tx_rejected)

    inOrder.verify(ledgerEndCache).set(lastOffset -> lastEventSeqId)
    inOrder.verify(dispatcher).signalNewHead(lastOffset)
    inOrder
      .verify(submissionTracker)
      .onCompletion(
        tx_accepted_completionDetails.completionStreamResponse -> tx_accepted_submitters
      )

    inOrder
      .verify(submissionTracker)
      .onCompletion(
        tx_rejected_completionDetails.completionStreamResponse -> tx_rejected_submitters
      )

    inOrder.verifyNoMoreInteractions()
  }
}

object InMemoryStateUpdaterSpec {

  import TraceContext.Implicits.Empty.*

  trait Scope
      extends Matchers
      with ScalaFutures
      with MockitoSugar
      with TestEssentials
      with HasExecutorServiceGeneric {

    override def handleFailure(message: String) = fail(message)

    val emptyArchiveToMetadata: (DamlLf.Archive, Time.Timestamp) => PackageMetadata = (_, _) =>
      PackageMetadata()
    val cacheUpdates = ArrayBuffer.empty[PrepareResult]
    val cachesUpdateCaptor =
      (v: PrepareResult) => cacheUpdates.addOne(v).pipe(_ => ())

    val inMemoryStateUpdater = InMemoryStateUpdaterFlow(
      2,
      executorService,
      executorService,
      FiniteDuration(10, "seconds"),
      Metrics.ForTesting,
      logger,
    )(
      prepare = (_, lastEventSequentialId) => result(lastEventSequentialId),
      update = cachesUpdateCaptor,
    )(emptyTraceContext)

    val txLogUpdate1 = Traced(
      TransactionLogUpdate.TransactionAccepted(
        transactionId = "tx1",
        commandId = "",
        workflowId = workflowId,
        effectiveAt = Timestamp.Epoch,
        offset = offset(1L),
        events = Vector(),
        completionDetails = None,
        domainId = Some(domainId1.toProtoPrimitive),
      )
    )(emptyTraceContext)

    val assignLogUpdate = Traced(
      TransactionLogUpdate.ReassignmentAccepted(
        updateId = "tx3",
        commandId = "",
        workflowId = workflowId,
        offset = offset(7L),
        completionDetails = None,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = SourceDomainId(domainId1),
          targetDomain = TargetDomainId(domainId2),
          submitter = Option(party1),
          reassignmentCounter = 15L,
          hostedStakeholders = party2 :: Nil,
          unassignId = CantonTimestamp.assertFromLong(155555L),
        ),
        reassignment = TransactionLogUpdate.ReassignmentAccepted.Assigned(
          CreatedEvent(
            eventOffset = offset(7L),
            transactionId = "tx3",
            nodeIndex = 0,
            eventSequentialId = 0,
            eventId = EventId(txId3, NodeId(0)),
            contractId = someCreateNode.coid,
            ledgerEffectiveTime = Timestamp.assertFromLong(12222),
            templateId = templateId,
            commandId = "",
            workflowId = workflowId,
            contractKey = None,
            treeEventWitnesses = Set.empty,
            flatEventWitnesses = Set(party2),
            submitters = Set.empty,
            createArgument =
              com.daml.lf.transaction.Versioned(someCreateNode.version, someCreateNode.arg),
            createSignatories = Set(party1),
            createObservers = Set(party2),
            createAgreementText = None,
            createKeyHash = None,
            createKey = None,
            createKeyMaintainers = None,
            driverMetadata = Some(someContractMetadataBytes),
          )
        ),
      )
    )(emptyTraceContext)

    val unassignLogUpdate = Traced(
      TransactionLogUpdate.ReassignmentAccepted(
        updateId = "tx4",
        commandId = "",
        workflowId = workflowId,
        offset = offset(8L),
        completionDetails = None,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = SourceDomainId(domainId2),
          targetDomain = TargetDomainId(domainId1),
          submitter = Option(party2),
          reassignmentCounter = 15L,
          hostedStakeholders = party1 :: Nil,
          unassignId = CantonTimestamp.assertFromLong(1555551L),
        ),
        reassignment = TransactionLogUpdate.ReassignmentAccepted.Unassigned(
          Reassignment.Unassign(
            contractId = someCreateNode.coid,
            templateId = templateId2,
            stakeholders = List(party2),
            assignmentExclusivity = Some(Timestamp.assertFromLong(123456L)),
          )
        ),
      )
    )(emptyTraceContext)

    val ledgerEndCache: MutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches: ContractStateCaches = mock[ContractStateCaches]
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView: StringInterningView = mock[StringInterningView]
    val dispatcherState: DispatcherState = mock[DispatcherState]
    val packageMetadataView: PackageMetadataView = mock[PackageMetadataView]
    val submissionTracker: SubmissionTracker = mock[SubmissionTracker]
    val dispatcher: Dispatcher[Offset] = mock[Dispatcher[Offset]]

    val inOrder: InOrder = inOrder(
      ledgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      packageMetadataView,
      submissionTracker,
      dispatcher,
    )

    when(dispatcherState.getDispatcher).thenReturn(dispatcher)

    val inMemoryState = new InMemoryState(
      ledgerEndCache = ledgerEndCache,
      contractStateCaches = contractStateCaches,
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
      packageMetadataView = packageMetadataView,
      submissionTracker = submissionTracker,
      loggerFactory = loggerFactory,
    )(executorService)

    val tx_accepted_commandId = "cAccepted"
    val tx_accepted_transactionId = "tAccepted"
    val tx_accepted_submitters: Set[String] = Set("p1", "p2")

    val tx_rejected_transactionId = "tRejected"
    val tx_rejected_submitters: Set[String] = Set("p3", "p4")

    val tx_accepted_completion: Completion = Completion(
      commandId = tx_accepted_commandId,
      applicationId = "appId",
      updateId = tx_accepted_transactionId,
      submissionId = "submissionId",
      actAs = Seq.empty,
    )
    val tx_rejected_completion: Completion =
      tx_accepted_completion.copy(updateId = tx_rejected_transactionId)
    val tx_accepted_completionDetails: TransactionLogUpdate.CompletionDetails =
      TransactionLogUpdate.CompletionDetails(
        completionStreamResponse =
          CompletionStreamResponse(completion = Some(tx_accepted_completion)),
        submitters = tx_accepted_submitters,
      )

    val tx_rejected_completionDetails: TransactionLogUpdate.CompletionDetails =
      TransactionLogUpdate.CompletionDetails(
        completionStreamResponse =
          CompletionStreamResponse(completion = Some(tx_rejected_completion)),
        submitters = tx_rejected_submitters,
      )

    val tx_accepted_withCompletionDetails_offset: Offset =
      Offset.fromHexString(Ref.HexString.assertFromString("aaaa"))

    val tx_accepted_withoutCompletionDetails_offset: Offset =
      Offset.fromHexString(Ref.HexString.assertFromString("bbbb"))

    val tx_rejected_offset: Offset =
      Offset.fromHexString(Ref.HexString.assertFromString("cccc"))

    val tx_accepted_withCompletionDetails: Traced[TransactionLogUpdate.TransactionAccepted] =
      Traced(
        TransactionLogUpdate.TransactionAccepted(
          transactionId = tx_accepted_transactionId,
          commandId = tx_accepted_commandId,
          workflowId = "wAccepted",
          effectiveAt = Timestamp.assertFromLong(1L),
          offset = tx_accepted_withCompletionDetails_offset,
          events = (1 to 3).map(_ => mock[TransactionLogUpdate.Event]).toVector,
          completionDetails = Some(tx_accepted_completionDetails),
          domainId = None,
        )
      )(emptyTraceContext)

    val tx_accepted_withoutCompletionDetails: Traced[TransactionLogUpdate.TransactionAccepted] =
      Traced(
        tx_accepted_withCompletionDetails.value.copy(
          completionDetails = None,
          offset = tx_accepted_withoutCompletionDetails_offset,
        )
      )(emptyTraceContext)

    val tx_rejected: Traced[TransactionLogUpdate.TransactionRejected] =
      Traced(
        TransactionLogUpdate.TransactionRejected(
          offset = tx_rejected_offset,
          completionDetails = tx_rejected_completionDetails,
        )
      )(emptyTraceContext)
    val packageMetadata: PackageMetadata = PackageMetadata(templates = Map(templatesForQn1))

    val lastOffset: Offset = tx_rejected_offset
    val lastEventSeqId = 123L
    val updates: Vector[Traced[TransactionLogUpdate]] =
      Vector(
        tx_accepted_withCompletionDetails,
        tx_accepted_withoutCompletionDetails,
        tx_rejected,
      )
    val prepareResult: PrepareResult = PrepareResult(
      updates = updates,
      lastOffset = lastOffset,
      lastEventSequentialId = lastEventSeqId,
      emptyTraceContext,
      packageMetadata = packageMetadata,
    )

    def result(lastEventSequentialId: Long): PrepareResult =
      PrepareResult(
        Vector.empty,
        offset(1L),
        lastEventSequentialId,
        emptyTraceContext,
        PackageMetadata(),
      )

    def runFlow(
        input: Seq[(Vector[(Offset, Traced[Update])], Long)]
    )(implicit mat: Materializer): Done =
      Source(input)
        .via(inMemoryStateUpdater)
        .runWith(Sink.ignore)
        .futureValue
  }

  private val participantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("EndlessReadServiceParticipant")

  private val txId1 = Ref.TransactionId.assertFromString("tx1")
  private val txId2 = Ref.TransactionId.assertFromString("tx2")
  private val txId3 = Ref.TransactionId.assertFromString("tx3")
  private val txId4 = Ref.TransactionId.assertFromString("tx4")

  private val domainId1 = DomainId.tryFromString("x::domainID1")
  private val domainId2 = DomainId.tryFromString("x::domainID2")

  private val party1 = Ref.Party.assertFromString("someparty1")
  private val party2 = Ref.Party.assertFromString("someparty2")

  private val templateQualifiedName1 = Ref.QualifiedName.assertFromString("Mod:I")
  private val templateQualifiedName2 = Ref.QualifiedName.assertFromString("Mod:I2")

  private val templateId = Identifier.assertFromString("pkgId1:Mod:I")
  private val templateId2 = Identifier.assertFromString("pkgId2:Mod:I2")

  private val templatesForQn1 = templateQualifiedName1 ->
    TemplatesForQualifiedName(
      NonEmptyUtil.fromUnsafe(Set(templateId)),
      TemplateIdWithPriority(templateId, Time.Timestamp.Epoch),
    )
  private val templatesForQn2 = templateQualifiedName2 ->
    TemplatesForQualifiedName(
      NonEmptyUtil.fromUnsafe(Set(templateId2)),
      TemplateIdWithPriority(templateId2, Time.Timestamp.Epoch),
    )

  private val someCreateNode = {
    val contractId = TransactionBuilder.newCid
    TestNodeBuilder
      .create(
        id = contractId,
        templateId = templateId,
        argument = Value.ValueUnit,
        signatories = Set(party1),
        observers = Set(party2),
      )
  }

  private val someContractMetadataBytes = Bytes.assertFromString("00aabb")

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

  private val update1 = offset(1L) -> Traced(
    Update.TransactionAccepted(
      completionInfoO = None,
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(TransactionBuilder.Empty),
      transactionId = txId1,
      recordTime = Timestamp.Epoch,
      divulgedContracts = List.empty,
      blindingInfoO = None,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
      domainId = domainId1,
    )
  )
  private val rawMetadataChangedUpdate = offset(2L) -> Update.ConfigurationChanged(
    Timestamp.Epoch,
    someSubmissionId,
    participantId,
    configuration,
  )
  private val metadataChangedUpdate = rawMetadataChangedUpdate.bimap(identity, Traced[Update])
  private val update3 = offset(3L) -> Traced[Update](
    Update.TransactionAccepted(
      completionInfoO = None,
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(TransactionBuilder.Empty),
      transactionId = txId2,
      recordTime = Timestamp.Epoch,
      divulgedContracts = List.empty,
      blindingInfoO = None,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
      domainId = DomainId.tryFromString("da::default"),
    )
  )
  private val update4 = offset(4L) -> Traced[Update](
    Update.CommandRejected(
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
      domainId = DomainId.tryFromString("da::default"),
    )
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

  private val update5 = offset(5L) -> Traced[Update](
    Update.PublicPackageUpload(
      archives = List(archive),
      sourceDescription = None,
      recordTime = Timestamp.Epoch,
      submissionId = None,
    )
  )

  private val update6 = offset(6L) -> Traced[Update](
    Update.PublicPackageUpload(
      archives = List(archive2),
      sourceDescription = None,
      recordTime = Timestamp.Epoch,
      submissionId = None,
    )
  )

  private val update7 = offset(7L) -> Traced[Update](
    Update.ReassignmentAccepted(
      optCompletionInfo = None,
      workflowId = Some(workflowId),
      updateId = txId3,
      recordTime = Timestamp.Epoch,
      reassignmentInfo = ReassignmentInfo(
        sourceDomain = SourceDomainId(domainId1),
        targetDomain = TargetDomainId(domainId2),
        submitter = Option(party1),
        reassignmentCounter = 15L,
        hostedStakeholders = party2 :: Nil,
        unassignId = CantonTimestamp.assertFromLong(155555L),
      ),
      reassignment = Reassignment.Assign(
        ledgerEffectiveTime = Timestamp.assertFromLong(12222),
        createNode = someCreateNode,
        contractMetadata = someContractMetadataBytes,
      ),
    )
  )

  private val update8 = offset(8L) -> Traced[Update](
    Update.ReassignmentAccepted(
      optCompletionInfo = None,
      workflowId = Some(workflowId),
      updateId = txId4,
      recordTime = Timestamp.Epoch,
      reassignmentInfo = ReassignmentInfo(
        sourceDomain = SourceDomainId(domainId2),
        targetDomain = TargetDomainId(domainId1),
        submitter = Option(party2),
        reassignmentCounter = 15L,
        hostedStakeholders = party1 :: Nil,
        unassignId = CantonTimestamp.assertFromLong(1555551L),
      ),
      reassignment = Reassignment.Unassign(
        contractId = someCreateNode.coid,
        templateId = templateId2,
        stakeholders = List(party2),
        assignmentExclusivity = Some(Timestamp.assertFromLong(123456L)),
      ),
    )
  )

  private val anotherMetadataChangedUpdate =
    rawMetadataChangedUpdate
      .bimap(
        Function.const(offset(5L)),
        _.copy(recordTime = Time.Timestamp.assertFromLong(1337L)),
      )
      .bimap(identity, Traced[Update](_))

  private def offset(idx: Long): Offset = {
    val base = BigInt(1) << 32
    Offset.fromByteArray((base + idx).toByteArray)
  }
}

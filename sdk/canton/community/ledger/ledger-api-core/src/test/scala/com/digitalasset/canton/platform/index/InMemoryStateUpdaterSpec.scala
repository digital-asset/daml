// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import cats.syntax.bifunctor.toBifunctorOps
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.CommandRejected.FinalReason
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  DomainIndex,
  Reassignment,
  ReassignmentInfo,
  RequestIndex,
  TransactionMeta,
  Update,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.index.InMemoryStateUpdater.PrepareResult
import com.digitalasset.canton.platform.index.InMemoryStateUpdaterSpec.*
import com.digitalasset.canton.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
  OffsetCheckpoint,
  OffsetCheckpointCache,
}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.CreatedEvent
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.{DispatcherState, InMemoryState}
import com.digitalasset.canton.protocol.{SourceDomainId, TargetDomainId}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorServiceGeneric,
  RequestCounter,
  SequencerCounter,
  TestEssentials,
}
import com.digitalasset.daml.lf.archive.DamlLf
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.ledger.EventId
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.CreateTransactionVersion
import com.digitalasset.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.digitalasset.daml.lf.transaction.{CommittedTransaction, NodeId, TransactionVersion}
import com.digitalasset.daml.lf.value.Value
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.mockito.{InOrder, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
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
        (Vector(update1, metadataChangedUpdate), 1L, CantonTimestamp.MinValue),
        (Vector(update3, update4), 3L, CantonTimestamp.MinValue.plusSeconds(50)),
      )
    )
    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(1L, CantonTimestamp.MinValue),
      result(3L, CantonTimestamp.MinValue.plusSeconds(50)),
    )
  }

  "flow" should "not process empty input batches" in new Scope {
    runFlow(
      Seq(
        // Empty input batch should have not effect
        (Vector.empty, 1L, CantonTimestamp.MinValue),
        (Vector(update3), 3L, CantonTimestamp.MinValue.plusSeconds(80)),
        (Vector(anotherMetadataChangedUpdate), 3L, CantonTimestamp.MinValue.plusSeconds(180)),
      )
    )

    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(3L, CantonTimestamp.MinValue.plusSeconds(80)),
      result(
        3L,
        CantonTimestamp.MinValue.plusSeconds(180),
      ), // Results in empty batch after processing
    )
  }

  "prepare" should "throw exception for an empty vector" in new Scope {
    an[NoSuchElementException] should be thrownBy {
      InMemoryStateUpdater.prepare(
        Vector.empty,
        0L,
        CantonTimestamp.MinValue,
      )
    }
  }

  "prepare" should "prepare a batch of a single update" in new Scope {
    InMemoryStateUpdater.prepare(
      Vector(update1),
      0L,
      CantonTimestamp.MinValue,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(1L),
      0L,
      CantonTimestamp.MinValue,
      update1._2.traceContext,
    )
  }

  "prepare" should "prepare a batch with reassignments" in new Scope {
    InMemoryStateUpdater.prepare(
      Vector(update1, update7, update8),
      0L,
      CantonTimestamp.MinValue.plusSeconds(10),
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1, assignLogUpdate, unassignLogUpdate),
      offset(8L),
      0L,
      CantonTimestamp.MinValue.plusSeconds(10),
      update1._2.traceContext,
    )
  }

  "prepare" should "set last offset and eventSequentialId to last element" in new Scope {
    InMemoryStateUpdater.prepare(
      Vector(update1, metadataChangedUpdate),
      6L,
      CantonTimestamp.MinValue,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      offset(2L),
      6L,
      CantonTimestamp.MinValue,
      metadataChangedUpdate._2.traceContext,
    )
  }

  "update" should "update the in-memory state" in new Scope {
    InMemoryStateUpdater.update(inMemoryState, logger)(prepareResult, false)

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

    inOrder.verify(ledgerEndCache).set((lastOffset, lastEventSeqId, lastPublicationTime))
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

  "update" should "update the in-memory state, but not the ledger-end and the dispatcher in repair mode" in new Scope {
    InMemoryStateUpdater.update(inMemoryState, logger)(prepareResult, true)

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

  "updateOffsetCheckpointCacheFlowWithTickingSource" should "not alter the original flow" in new Scope {
    implicit val ec: ExecutionContext = executorService

    // here we define the offset, domainId and recordTime for each offset-update pair the same way they arrive to the flow as Some values
    // the None values denote the ticks arrived that are used to update the offset checkpoint cache
    val offsetsAndTicks =
      Seq(Some(1L), Some(2L), None, None, Some(3L), Some(4L), Some(5L), None, Some(6L), None)
    val domainIdsAndTicks =
      Seq(Some(1L), Some(2L), None, None, Some(2L), Some(1L), Some(3L), None, Some(1L), None)
    val recordTimesAndTicks = offsetsAndTicks

    val offsetCheckpointsExpected =
      Seq(
        // offset -> Map[domain, time]
        2 -> Map(
          1 -> 1,
          2 -> 2,
        ),
        2 -> Map(
          1 -> 1,
          2 -> 2,
        ),
        5 -> Map(
          1 -> 4,
          2 -> 3,
          3 -> 5,
        ),
        6 -> Map(
          1 -> 6,
          2 -> 3,
          3 -> 5,
        ),
      ).map { case (offset, domainTimesRaw) =>
        OffsetCheckpoint(
          offset = Offset.fromLong(offset.toLong),
          domainTimes = domainTimesRaw.map { case (d, t) =>
            DomainId.tryFromString(d.toString + "::default") -> Timestamp(t.toLong)
          },
        )
      }

    val input = createInputSeq(
      offsetsAndTicks,
      domainIdsAndTicks,
      recordTimesAndTicks,
    )

    val (expectedOutput, output, checkpoints) =
      runUpdateOffsetCheckpointCacheFlow(
        input
      ).futureValue

    output shouldBe expectedOutput
    checkpoints shouldBe findCheckpointOffsets(input)
    checkpoints shouldBe offsetCheckpointsExpected

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

    val cacheUpdates = ArrayBuffer.empty[PrepareResult]
    val cachesUpdateCaptor =
      (v: PrepareResult, _: Boolean) => cacheUpdates.addOne(v).pipe(_ => ())

    val inMemoryStateUpdater = InMemoryStateUpdaterFlow(
      prepareUpdatesParallelism = 2,
      prepareUpdatesExecutionContext = executorService,
      updateCachesExecutionContext = executorService,
      preparePackageMetadataTimeOutWarning = FiniteDuration(10, "seconds"),
      offsetCheckpointCacheUpdateInterval = FiniteDuration(15, "seconds"),
      metrics = LedgerApiServerMetrics.ForTesting,
      logger = logger,
    )(
      inMemoryState = inMemoryState,
      prepare = (_, lastEventSequentialId, lastPublicationTime) =>
        result(lastEventSequentialId, lastPublicationTime),
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
        domainId = domainId1.toProtoPrimitive,
        recordTime = Timestamp.Epoch,
      )
    )(emptyTraceContext)

    val assignLogUpdate = Traced(
      TransactionLogUpdate.ReassignmentAccepted(
        updateId = "tx3",
        commandId = "",
        workflowId = workflowId,
        offset = offset(7L),
        recordTime = Timestamp.Epoch,
        completionDetails = None,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = SourceDomainId(domainId1),
          targetDomain = TargetDomainId(domainId2),
          submitter = Option(party1),
          reassignmentCounter = 15L,
          hostedStakeholders = party2 :: Nil,
          unassignId = CantonTimestamp.assertFromLong(155555L),
          isTransferringParticipant = true,
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
            packageName = packageName,
            packageVersion = Some(packageVersion),
            commandId = "",
            workflowId = workflowId,
            contractKey = None,
            treeEventWitnesses = Set.empty,
            flatEventWitnesses = Set(party2),
            submitters = Set.empty,
            createArgument = com.digitalasset.daml.lf.transaction
              .Versioned(someCreateNode.version, someCreateNode.arg),
            createSignatories = Set(party1),
            createObservers = Set(party2),
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
        recordTime = Timestamp.Epoch,
        completionDetails = None,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = SourceDomainId(domainId2),
          targetDomain = TargetDomainId(domainId1),
          submitter = Option(party2),
          reassignmentCounter = 15L,
          hostedStakeholders = party1 :: Nil,
          unassignId = CantonTimestamp.assertFromLong(1555551L),
          isTransferringParticipant = true,
        ),
        reassignment = TransactionLogUpdate.ReassignmentAccepted.Unassigned(
          Reassignment.Unassign(
            contractId = someCreateNode.coid,
            templateId = templateId2,
            packageName = packageName,
            stakeholders = List(party2),
            assignmentExclusivity = Some(Timestamp.assertFromLong(123456L)),
          )
        ),
      )
    )(emptyTraceContext)

    val ledgerEndCache: MutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches: ContractStateCaches = mock[ContractStateCaches]
    val offsetCheckpointCache: OffsetCheckpointCache = mock[OffsetCheckpointCache]
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView: StringInterningView = mock[StringInterningView]
    val dispatcherState: DispatcherState = mock[DispatcherState]
    val submissionTracker: SubmissionTracker = mock[SubmissionTracker]
    val dispatcher: Dispatcher[Offset] = mock[Dispatcher[Offset]]
    val commandProgressTracker = CommandProgressTracker.NoOp

    val inOrder: InOrder = inOrder(
      ledgerEndCache,
      contractStateCaches,
      inMemoryFanoutBuffer,
      stringInterningView,
      dispatcherState,
      submissionTracker,
      dispatcher,
    )

    when(dispatcherState.getDispatcher).thenReturn(dispatcher)

    val inMemoryState = new InMemoryState(
      ledgerEndCache = ledgerEndCache,
      contractStateCaches = contractStateCaches,
      offsetCheckpointCache = offsetCheckpointCache,
      inMemoryFanoutBuffer = inMemoryFanoutBuffer,
      stringInterningView = stringInterningView,
      dispatcherState = dispatcherState,
      submissionTracker = submissionTracker,
      commandProgressTracker = commandProgressTracker,
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
        completionStreamResponse = CompletionStreamResponse(completionResponse =
          CompletionStreamResponse.CompletionResponse.Completion(tx_accepted_completion)
        ),
        submitters = tx_accepted_submitters,
      )

    val tx_rejected_completionDetails: TransactionLogUpdate.CompletionDetails =
      TransactionLogUpdate.CompletionDetails(
        completionStreamResponse = CompletionStreamResponse(completionResponse =
          CompletionStreamResponse.CompletionResponse.Completion(tx_rejected_completion)
        ),
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
          domainId = domainId1.toProtoPrimitive,
          recordTime = Timestamp(1),
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

    val lastOffset: Offset = tx_rejected_offset
    val lastEventSeqId = 123L
    val lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(1000)
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
      lastPublicationTime = lastPublicationTime,
      emptyTraceContext,
    )

    def result(lastEventSequentialId: Long, publicationTime: CantonTimestamp): PrepareResult =
      PrepareResult(
        Vector.empty,
        offset(1L),
        lastEventSequentialId,
        lastPublicationTime,
        emptyTraceContext,
      )

    def runFlow(
        input: Seq[(Vector[(Offset, Traced[Update])], Long, CantonTimestamp)]
    )(implicit mat: Materializer): Done =
      Source(input)
        .via(inMemoryStateUpdater(false))
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

  private val templateId = Identifier.assertFromString("pkgId1:Mod:I")
  private val templateId2 = Identifier.assertFromString("pkgId2:Mod:I2")

  private val packageName = Ref.PackageName.assertFromString("pkg-name")
  private val packageVersion = Ref.PackageVersion.assertFromString("1.2.3")

  private val someCreateNode = {
    val contractId = TransactionBuilder.newCid
    TestNodeBuilder
      .create(
        id = contractId,
        packageName = packageName,
        packageVersion = Some(packageVersion),
        templateId = templateId,
        argument = Value.ValueUnit,
        signatories = Set(party1),
        observers = Set(party2),
        version = CreateTransactionVersion.Version(TransactionVersion.VDev),
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
      blindingInfoO = None,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
      domainId = domainId1,
      Some(
        DomainIndex.of(
          RequestIndex(RequestCounter(1), Some(SequencerCounter(1)), CantonTimestamp.MinValue)
        )
      ),
    )
  )
  private val rawMetadataChangedUpdate = offset(2L) -> Update.Init(
    Timestamp.Epoch
  )
  private val metadataChangedUpdate = rawMetadataChangedUpdate.bimap(identity, Traced[Update])
  private val update3 = offset(3L) -> Traced[Update](
    Update.TransactionAccepted(
      completionInfoO = None,
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(TransactionBuilder.Empty),
      transactionId = txId2,
      recordTime = Timestamp.Epoch,
      blindingInfoO = None,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
      domainId = DomainId.tryFromString("da::default"),
      Some(
        DomainIndex.of(
          RequestIndex(RequestCounter(1), Some(SequencerCounter(1)), CantonTimestamp.MinValue)
        )
      ),
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
        None,
      ),
      reasonTemplate = FinalReason(new Status()),
      domainId = DomainId.tryFromString("da::default"),
      Some(
        DomainIndex.of(
          RequestIndex(RequestCounter(1), Some(SequencerCounter(1)), CantonTimestamp.MinValue)
        )
      ),
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
        isTransferringParticipant = true,
      ),
      reassignment = Reassignment.Assign(
        ledgerEffectiveTime = Timestamp.assertFromLong(12222),
        createNode = someCreateNode,
        contractMetadata = someContractMetadataBytes,
      ),
      Some(
        DomainIndex.of(
          RequestIndex(RequestCounter(1), Some(SequencerCounter(1)), CantonTimestamp.MinValue)
        )
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
        isTransferringParticipant = true,
      ),
      reassignment = Reassignment.Unassign(
        contractId = someCreateNode.coid,
        templateId = templateId2,
        packageName = packageName,
        stakeholders = List(party2),
        assignmentExclusivity = Some(Timestamp.assertFromLong(123456L)),
      ),
      Some(
        DomainIndex.of(
          RequestIndex(RequestCounter(1), Some(SequencerCounter(1)), CantonTimestamp.MinValue)
        )
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

  // traverse the list from left to right and if a None is found add the exact previous checkpoint in the result
  private def findCheckpointOffsets(
      input: Seq[Option[(Offset, Traced[Update.TransactionAccepted])]]
  ): Seq[OffsetCheckpoint] =
    input
      .foldLeft[(Seq[OffsetCheckpoint], Option[OffsetCheckpoint])]((Seq.empty, None)) {
        // new update and offset pair received update offsetCheckpoint
        case ((acc, lastCheckpointO), Some((currOffset, Traced(update)))) =>
          (
            acc,
            Some(
              OffsetCheckpoint(
                offset = currOffset,
                domainTimes = lastCheckpointO
                  .map(_.domainTimes)
                  .getOrElse(Map.empty[DomainId, Timestamp])
                  .updated(update.domainId, update.recordTime),
              )
            ),
          )
        // tick received add checkpoint to the seq, if there is one
        case ((acc, Some(lastCheckpoint)), None) => (acc :+ lastCheckpoint, Some(lastCheckpoint))
        case ((acc, None), None) => (acc, None)
      }
      ._1

  private def createInputSeq(
      offsetsAndTicks: Seq[Option[Long]],
      domainIdsAndTicks: Seq[Option[Long]],
      recordTimesAndTicks: Seq[Option[Long]],
  ): Seq[Option[(Offset, Traced[Update.TransactionAccepted])]] = {
    val offsets = offsetsAndTicks.map(_.map(Offset.fromLong))
    val domainIds =
      domainIdsAndTicks.map(_.map(x => DomainId.tryFromString(x.toString + "::default")))

    val updatesSeq: Seq[Option[Traced[Update.TransactionAccepted]]] =
      recordTimesAndTicks.zip(domainIds).map {
        case (None, _) => None
        case (_, None) => None
        case (Some(t), Some(domain)) =>
          Some(
            Traced.empty(
              Update.TransactionAccepted(
                completionInfoO = None,
                transactionMeta = someTransactionMeta,
                transaction = CommittedTransaction(TransactionBuilder.Empty),
                transactionId = txId1,
                recordTime = Timestamp(t),
                blindingInfoO = None,
                hostedWitnesses = Nil,
                contractMetadata = Map.empty,
                domainId = domain,
                domainIndex = None,
              )
            )
          )
      }

    offsets.zip(updatesSeq).map {
      case (None, _) => None
      case (_, None) => None
      case (Some(offset), Some(tracedUpdate)) => Some((offset, tracedUpdate))
    }

  }

  def runUpdateOffsetCheckpointCacheFlow(
      inputSeq: Seq[Option[(Offset, Traced[Update.TransactionAccepted])]]
  )(implicit materializer: Materializer, ec: ExecutionContext) = {
    val elementsQueue =
      new ConcurrentLinkedQueue[Option[(Offset, Traced[Update.TransactionAccepted])]]
    inputSeq.foreach(elementsQueue.add)

    val flattenedSeq: Seq[(Vector[(Offset, Traced[Update])], Long, CantonTimestamp)] =
      inputSeq.flatten.map(Vector(_)).map((_, 1L, CantonTimestamp.MinValue))

    val bufferSize = 100
    val (sourceQueueSomes, sourceSomes) = Source
      .queue[(Vector[(Offset, Traced[Update])], Long, CantonTimestamp)](bufferSize)
      .preMaterialize()
    val (sourceQueueNones, sourceNones) = Source
      .queue[Option[Nothing]](bufferSize)
      .preMaterialize()

    def offerNext() =
      Option(elementsQueue.poll()) match {
        // send element
        case Some(Some(pair)) =>
          sourceQueueSomes.offer((Vector(pair), 1L, CantonTimestamp.MinValue))
        // send tick
        case Some(None) =>
          sourceQueueNones.offer(None)
        // queue is empty send finished message
        case None => sourceQueueSomes.complete()
      }
    offerNext()

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var checkpoints: Seq[OffsetCheckpoint] = Seq.empty

    val output = sourceSomes
      .via(
        InMemoryStateUpdaterFlow
          .updateOffsetCheckpointCacheFlowWithTickingSource(
            updateOffsetCheckpointCache = oc => {
              checkpoints = checkpoints :+ oc
              offerNext()
            },
            tick = sourceNones,
          )
      )
      .alsoTo(Sink.foreach(_ => offerNext()))
      .runWith(Sink.seq)

    output.map(o => (flattenedSeq, o, checkpoints))

  }
}

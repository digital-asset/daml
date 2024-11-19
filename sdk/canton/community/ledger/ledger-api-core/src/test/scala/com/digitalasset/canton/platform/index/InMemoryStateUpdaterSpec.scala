// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import cats.data.NonEmptyVector
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.Update.CommandRejected.FinalReason
import com.digitalasset.canton.ledger.participant.state.{
  CompletionInfo,
  Reassignment,
  ReassignmentInfo,
  TransactionMeta,
  Update,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.index.InMemoryStateUpdater.PrepareResult
import com.digitalasset.canton.platform.index.InMemoryStateUpdaterSpec.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
  OffsetCheckpoint,
  OffsetCheckpointCache,
}
import com.digitalasset.canton.platform.store.dao.events.ContractStateEvent
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate.CreatedEvent
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.{DispatcherState, InMemoryState}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.canton.{
  BaseTest,
  HasExecutorServiceGeneric,
  RequestCounter,
  SequencerCounter,
  TestEssentials,
  data,
}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.ledger.EventId
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.CreateTransactionVersion
import com.digitalasset.daml.lf.transaction.test.{TestNodeBuilder, TransactionBuilder}
import com.digitalasset.daml.lf.transaction.{CommittedTransaction, Node, NodeId}
import com.digitalasset.daml.lf.value.Value
import com.google.rpc.status.Status
import org.apache.pekko.Done
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.mockito.matchers.DefaultValueProvider
import org.mockito.{InOrder, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.*

class InMemoryStateUpdaterSpec
    extends AnyFlatSpec
    with Matchers
    with PekkoBeforeAndAfterAll
    with MockitoSugar
    with BaseTest {

  "flow" should "correctly process updates in order" in new Scope {
    val secondLedgerEnd = someLedgerEnd.copy(
      lastOffset = AbsoluteOffset.tryFromLong(12),
      lastEventSeqId = 12L,
    )
    runFlow(
      Seq(
        (Vector(update1, metadataChangedUpdate), someLedgerEnd),
        (Vector(update3, update4), secondLedgerEnd),
      )
    )
    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(someLedgerEnd),
      result(secondLedgerEnd),
    )
  }

  "flow" should "not process empty input batches" in new Scope {
    val secondLedgerEnd = someLedgerEnd.copy(
      lastOffset = AbsoluteOffset.tryFromLong(12),
      lastEventSeqId = 12L,
    )
    val thirdLedgerEnd = someLedgerEnd.copy(
      lastOffset = AbsoluteOffset.tryFromLong(14),
      lastEventSeqId = 14L,
      lastPublicationTime = CantonTimestamp.assertFromLong(15L),
    )
    runFlow(
      Seq(
        // Empty input batch should have not effect
        (Vector.empty, someLedgerEnd),
        (Vector(update3), secondLedgerEnd),
        (Vector(anotherMetadataChangedUpdate), thirdLedgerEnd),
      )
    )

    cacheUpdates should contain theSameElementsInOrderAs Seq(
      result(secondLedgerEnd),
      result(thirdLedgerEnd),
      // Results in empty batch after processing
    )
  }

  "prepare" should "throw exception for an empty vector" in new Scope {
    an[NoSuchElementException] should be thrownBy {
      InMemoryStateUpdater.prepare(
        Vector.empty,
        someLedgerEnd,
      )
    }
  }

  "prepare" should "prepare a batch of a single update" in new Scope {
    InMemoryStateUpdater.prepare(
      Vector(update1),
      someLedgerEnd,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      someLedgerEnd,
      update1._2.traceContext,
    )
  }

  "prepare" should "prepare a batch with reassignments" in new Scope {
    InMemoryStateUpdater.prepare(
      Vector(update1, update7, update8),
      someLedgerEnd,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1, assignLogUpdate, unassignLogUpdate),
      someLedgerEnd,
      update1._2.traceContext,
    )
  }

  "prepare" should "set last offset and eventSequentialId to last element" in new Scope {
    InMemoryStateUpdater.prepare(
      Vector(update1, metadataChangedUpdate),
      someLedgerEnd,
    ) shouldBe PrepareResult(
      Vector(txLogUpdate1),
      someLedgerEnd,
      metadataChangedUpdate._2.traceContext,
    )
  }

  "update" should "update the in-memory state" in new Scope {
    InMemoryStateUpdater.update(inMemoryState, logger)(prepareResult, false)

    inOrder
      .verify(inMemoryFanoutBuffer)
      .push(
        tx_accepted_withCompletionStreamResponse
      )
    inOrder
      .verify(contractStateCaches)
      .push(any[NonEmptyVector[ContractStateEvent]])(any[TraceContext])
    inOrder
      .verify(inMemoryFanoutBuffer)
      .push(
        tx_accepted_withoutCompletionStreamResponse
      )
    inOrder.verify(inMemoryFanoutBuffer).push(tx_rejected)

    inOrder
      .verify(ledgerEndCache)
      .set(Some(lastLedgerEnd))
    inOrder.verify(dispatcher).signalNewHead(lastOffset)
    inOrder
      .verify(submissionTracker)
      .onCompletion(tx_accepted_completionStreamResponse)

    inOrder
      .verify(submissionTracker)
      .onCompletion(tx_rejected_completionStreamResponse)

    inOrder.verifyNoMoreInteractions()
  }

  "update" should "update the in-memory state, but not the ledger-end and the dispatcher in repair mode" in new Scope {
    InMemoryStateUpdater.update(inMemoryState, logger)(prepareResult, true)

    inOrder
      .verify(inMemoryFanoutBuffer)
      .push(
        tx_accepted_withCompletionStreamResponse
      )
    inOrder
      .verify(contractStateCaches)
      .push(any[NonEmptyVector[ContractStateEvent]])(any[TraceContext])
    inOrder
      .verify(inMemoryFanoutBuffer)
      .push(
        tx_accepted_withoutCompletionStreamResponse
      )
    inOrder.verify(inMemoryFanoutBuffer).push(tx_rejected)

    inOrder
      .verify(submissionTracker)
      .onCompletion(tx_accepted_completionStreamResponse)

    inOrder
      .verify(submissionTracker)
      .onCompletion(tx_rejected_completionStreamResponse)

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
          offset = AbsoluteOffset.tryFromLong(offset.toLong),
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

  "updateOffsetCheckpointCacheFlowWithTickingSource" should "update the domain time for all the Update types that contain one" in new Scope {
    implicit val ec: ExecutionContext = executorService

    private val updatesSeq: Seq[Update] = Seq(
      transactionAccepted(1, domainId1),
      assignmentAccepted(2, source = domainId2, target = domainId1),
      unassignmentAccepted(3, source = domainId1, target = domainId2),
      commandRejected(4, domainId1),
      sequencerIndexMoved(5, domainId1),
    )

    private val offsets = (1L to updatesSeq.length.toLong).map(AbsoluteOffset.tryFromLong)
    private val updatesWithOffsets = offsets.zip(updatesSeq)

    // tick after each update to have one checkpoint after every update
    // the None values denote the ticks arrived that are used to update the offset checkpoint cache
    private val input =
      updatesWithOffsets.flatMap(elem => Seq(Some(elem), None))

    private val (expectedOutput, output, checkpoints) =
      runUpdateOffsetCheckpointCacheFlow(
        input
      ).futureValue

    private val offsetCheckpointsExpected =
      Seq(
        // offset -> Map[domain, time]
        1 -> Map(
          domainId1 -> 1
        ),
        2 -> Map(
          domainId1 -> 2
        ),
        3 -> Map(
          domainId1 -> 3
        ),
        4 -> Map(
          domainId1 -> 4
        ),
        5 -> Map(
          domainId1 -> 5
        ),
      ).map { case (offset, domainTimesRaw) =>
        OffsetCheckpoint(
          offset = AbsoluteOffset.tryFromLong(offset.toLong),
          domainTimes = domainTimesRaw.map { case (d, t) =>
            d -> Timestamp(t.toLong)
          },
        )
      }

    output shouldBe expectedOutput
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
      prepare = (_, ledgerEnd) => result(ledgerEnd),
      update = cachesUpdateCaptor,
    )(emptyTraceContext)

    val txLogUpdate1 =
      TransactionLogUpdate.TransactionAccepted(
        updateId = "tx1",
        commandId = "",
        workflowId = workflowId,
        effectiveAt = Timestamp.Epoch,
        offset = offset(11L),
        events = Vector(),
        completionStreamResponse = None,
        domainId = domainId1.toProtoPrimitive,
        recordTime = Timestamp.Epoch,
      )(emptyTraceContext)

    val assignLogUpdate =
      TransactionLogUpdate.ReassignmentAccepted(
        updateId = "tx3",
        commandId = "",
        workflowId = workflowId,
        offset = offset(17L),
        recordTime = Timestamp.Epoch,
        completionStreamResponse = None,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = ReassignmentTag.Source(domainId1),
          targetDomain = ReassignmentTag.Target(domainId2),
          submitter = Option(party1),
          reassignmentCounter = 15L,
          hostedStakeholders = party2 :: Nil,
          unassignId = CantonTimestamp.assertFromLong(155555L),
          isReassigningParticipant = true,
        ),
        reassignment = TransactionLogUpdate.ReassignmentAccepted.Assigned(
          CreatedEvent(
            eventOffset = offset(17L),
            updateId = "tx3",
            nodeIndex = 0,
            eventSequentialId = 0,
            eventId = EventId(txId3, NodeId(0)),
            contractId = someCreateNode.coid,
            ledgerEffectiveTime = Timestamp.assertFromLong(12222),
            templateId = someCreateNode.templateId,
            packageName = someCreateNode.packageName,
            packageVersion = someCreateNode.packageVersion,
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
            driverMetadata = someContractMetadataBytes,
          )
        ),
      )(emptyTraceContext)

    val unassignLogUpdate =
      TransactionLogUpdate.ReassignmentAccepted(
        updateId = "tx4",
        commandId = "",
        workflowId = workflowId,
        offset = offset(18L),
        recordTime = Timestamp.Epoch,
        completionStreamResponse = None,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = ReassignmentTag.Source(domainId2),
          targetDomain = ReassignmentTag.Target(domainId1),
          submitter = Option(party2),
          reassignmentCounter = 15L,
          hostedStakeholders = party1 :: Nil,
          unassignId = CantonTimestamp.assertFromLong(1555551L),
          isReassigningParticipant = true,
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
      )(emptyTraceContext)

    val ledgerEndCache: MutableLedgerEndCache = mock[MutableLedgerEndCache]
    val contractStateCaches: ContractStateCaches = mock[ContractStateCaches]
    val offsetCheckpointCache: OffsetCheckpointCache = mock[OffsetCheckpointCache]
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer = mock[InMemoryFanoutBuffer]
    val stringInterningView: StringInterningView = mock[StringInterningView]
    val dispatcherState: DispatcherState = mock[DispatcherState]
    val submissionTracker: SubmissionTracker = mock[SubmissionTracker]
    val dispatcher: Dispatcher[AbsoluteOffset] = mock[Dispatcher[AbsoluteOffset]]
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
    val tx_accepted_updateId = "tAccepted"
    val tx_accepted_submitters: Set[String] = Set("p1", "p2")

    val tx_rejected_updateId = "tRejected"
    val tx_rejected_submitters: Set[String] = Set("p3", "p4")

    val tx_accepted_completion: Completion = Completion(
      commandId = tx_accepted_commandId,
      applicationId = "appId",
      updateId = tx_accepted_updateId,
      submissionId = "submissionId",
      actAs = tx_accepted_submitters.toSeq,
    )
    val tx_rejected_completion: Completion =
      tx_accepted_completion.copy(
        updateId = tx_rejected_updateId,
        actAs = tx_rejected_submitters.toSeq,
      )
    val tx_accepted_completionStreamResponse: CompletionStreamResponse =
      CompletionStreamResponse(
        CompletionStreamResponse.CompletionResponse.Completion(
          tx_accepted_completion
        )
      )

    val tx_rejected_completionStreamResponse =
      CompletionStreamResponse(
        CompletionStreamResponse.CompletionResponse.Completion(
          tx_rejected_completion
        )
      )

    val tx_accepted_withCompletionStreamResponse_offset: Offset =
      Offset.fromHexString(Ref.HexString.assertFromString("aaaa"))

    val tx_accepted_withoutCompletionStreamResponse_offset: Offset =
      Offset.fromHexString(Ref.HexString.assertFromString("bbbb"))

    val tx_rejected_offset_old: Offset =
      Offset.fromLong(3333L)

    val tx_rejected_offset: AbsoluteOffset = AbsoluteOffset.tryFromLong(3333L)

    val tx_accepted_withCompletionStreamResponse: TransactionLogUpdate.TransactionAccepted =
      TransactionLogUpdate.TransactionAccepted(
        updateId = tx_accepted_updateId,
        commandId = tx_accepted_commandId,
        workflowId = "wAccepted",
        effectiveAt = Timestamp.assertFromLong(1L),
        offset = tx_accepted_withCompletionStreamResponse_offset,
        events = (1 to 3)
          .map(i =>
            toCreatedEvent(
              genCreateNode,
              tx_accepted_withCompletionStreamResponse_offset,
              Ref.TransactionId.assertFromString(tx_accepted_updateId),
              NodeId(i),
            )
          )
          .toVector,
        completionStreamResponse = Some(tx_accepted_completionStreamResponse),
        domainId = domainId1.toProtoPrimitive,
        recordTime = Timestamp(1),
      )(emptyTraceContext)

    val tx_accepted_withoutCompletionStreamResponse: TransactionLogUpdate.TransactionAccepted =
      tx_accepted_withCompletionStreamResponse.copy(
        completionStreamResponse = None,
        offset = tx_accepted_withoutCompletionStreamResponse_offset,
      )(emptyTraceContext)

    val tx_rejected: TransactionLogUpdate.TransactionRejected =
      TransactionLogUpdate.TransactionRejected(
        offset = tx_rejected_offset_old,
        completionStreamResponse = tx_rejected_completionStreamResponse,
      )(emptyTraceContext)

    val lastOffset: AbsoluteOffset = tx_rejected_offset
    val lastEventSeqId = 123L
    val lastPublicationTime = CantonTimestamp.MinValue.plusSeconds(1000)
    val lastStringInterningId = 234
    val lastLedgerEnd = LedgerEnd(
      lastOffset = lastOffset,
      lastEventSeqId = lastEventSeqId,
      lastStringInterningId = lastStringInterningId,
      lastPublicationTime = lastPublicationTime,
    )
    val updates: Vector[TransactionLogUpdate] =
      Vector(
        tx_accepted_withCompletionStreamResponse,
        tx_accepted_withoutCompletionStreamResponse,
        tx_rejected,
      )
    val prepareResult: PrepareResult = PrepareResult(
      updates = updates,
      ledgerEnd = lastLedgerEnd,
      emptyTraceContext,
    )

    def result(ledgerEnd: LedgerEnd): PrepareResult =
      PrepareResult(
        Vector.empty,
        ledgerEnd,
        emptyTraceContext,
      )

    def runFlow(
        input: Seq[(Vector[(AbsoluteOffset, Update)], LedgerEnd)]
    )(implicit mat: Materializer): Done =
      Source(input)
        .via(inMemoryStateUpdater(false))
        .runWith(Sink.ignore)
        .futureValue
  }

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

  private def genCreateNode = {
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
        version = CreateTransactionVersion.Version(LanguageVersion.v2_dev),
      )
  }
  private val someCreateNode = genCreateNode

  private def toCreatedEvent(
      createdNode: Node.Create,
      txOffset: Offset,
      updateId: data.UpdateId,
      nodeId: NodeId,
  ) =
    CreatedEvent(
      eventOffset = txOffset,
      updateId = updateId,
      nodeIndex = nodeId.index,
      eventSequentialId = 0,
      eventId = EventId(updateId, nodeId),
      contractId = createdNode.coid,
      ledgerEffectiveTime = Timestamp.assertFromLong(12222),
      templateId = createdNode.templateId,
      packageName = createdNode.packageName,
      packageVersion = createdNode.packageVersion,
      commandId = "",
      workflowId = workflowId,
      contractKey = None,
      treeEventWitnesses = Set.empty,
      flatEventWitnesses = createdNode.stakeholders,
      submitters = Set.empty,
      createArgument = com.digitalasset.daml.lf.transaction
        .Versioned(createdNode.version, createdNode.arg),
      createSignatories = createdNode.signatories,
      createObservers = createdNode.stakeholders.diff(createdNode.signatories),
      createKeyHash = createdNode.keyOpt.map(_.globalKey.hash),
      createKey = createdNode.keyOpt.map(_.globalKey),
      createKeyMaintainers = createdNode.keyOpt.map(_.maintainers),
      driverMetadata = someContractMetadataBytes,
    )

  implicit val defaultValueProviderCreatedEvent
      : DefaultValueProvider[NonEmptyVector[ContractStateEvent]] =
    new DefaultValueProvider[NonEmptyVector[ContractStateEvent]] {
      override def default: NonEmptyVector[ContractStateEvent] =
        NonEmptyVector.one(
          InMemoryStateUpdater.convertLogToStateEvent(
            toCreatedEvent(
              genCreateNode,
              Offset.firstOffset,
              Ref.TransactionId.assertFromString("yolo"),
              NodeId(0),
            )
          )
        )
    }

  private val someContractMetadataBytes = Bytes.assertFromString("00aabb")

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

  private val update1 = absoluteOffset(11L) -> transactionAccepted(t = 0L, domainId = domainId1)
  private def rawMetadataChangedUpdate(offset: AbsoluteOffset, recordTime: Timestamp) =
    offset ->
      Update.SequencerIndexMoved(
        domainId = DomainId.tryFromString("x::domain"),
        sequencerCounter = SequencerCounter(15L),
        recordTime = CantonTimestamp(recordTime),
        requestCounterO = None,
      )

  private val metadataChangedUpdate = rawMetadataChangedUpdate(absoluteOffset(12L), Timestamp.Epoch)
  private val update3 = absoluteOffset(13L) ->
    Update.SequencedTransactionAccepted(
      completionInfoO = None,
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(TransactionBuilder.Empty),
      updateId = txId2,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
      domainId = DomainId.tryFromString("da::default"),
      requestCounter = RequestCounter(1),
      sequencerCounter = SequencerCounter(1),
      recordTime = CantonTimestamp.MinValue,
    )

  private val update4 = absoluteOffset(14L) ->
    commandRejected(t = 1337L, domainId = DomainId.tryFromString("da::default"))

  private val update7 = absoluteOffset(17L) ->
    assignmentAccepted(t = 0, source = domainId1, target = domainId2)

  private val update8 = absoluteOffset(18L) ->
    unassignmentAccepted(t = 0, source = domainId2, target = domainId1)

  private val anotherMetadataChangedUpdate =
    rawMetadataChangedUpdate(absoluteOffset(15L), Timestamp.assertFromLong(1337L))

  private def offset(idx: Long): Offset = {
    val base = 1000000000L
    Offset.fromLong(base + idx)
  }

  private def absoluteOffset(idx: Long): AbsoluteOffset =
    AbsoluteOffset.tryFromLong(1000000000L + idx)

  // traverse the list from left to right and if a None is found add the exact previous checkpoint in the result
  private def findCheckpointOffsets(
      input: Seq[Option[(AbsoluteOffset, Update.TransactionAccepted)]]
  ): Seq[OffsetCheckpoint] =
    input
      .foldLeft[(Seq[OffsetCheckpoint], Option[OffsetCheckpoint])]((Seq.empty, None)) {
        // new update and offset pair received update offsetCheckpoint
        case ((acc, lastCheckpointO), Some((currOffset, update))) =>
          (
            acc,
            Some(
              OffsetCheckpoint(
                offset = currOffset,
                domainTimes = lastCheckpointO
                  .map(_.domainTimes)
                  .getOrElse(Map.empty[DomainId, Timestamp])
                  .updated(update.domainId, update.recordTime.toLf),
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
  ): Seq[Option[(AbsoluteOffset, Update.TransactionAccepted)]] = {
    val offsets = offsetsAndTicks.map(_.map(AbsoluteOffset.tryFromLong))
    val domainIds =
      domainIdsAndTicks.map(_.map(x => DomainId.tryFromString(x.toString + "::default")))

    val updatesSeq: Seq[Option[Update.TransactionAccepted]] =
      recordTimesAndTicks.zip(domainIds).map {
        case (Some(t), Some(domain)) =>
          Some(
            transactionAccepted(t, domain)
          )
        case _ => None
      }

    offsets.zip(updatesSeq).map {
      case (Some(offset), Some(tracedUpdate)) => Some((offset, tracedUpdate))
      case _ => None
    }

  }

  // this function gets a sequence of offset, update pairs as Some values
  // and ticks as Nones
  // runs the updateOffsetCheckpointCacheFlowWithTickingSource
  // and provides as output:
  //  - 1. the expected output
  //  - 2. the actual output
  //  - 3. the checkpoints updates in the offset checkpoint cache
  def runUpdateOffsetCheckpointCacheFlow(
      inputSeq: Seq[Option[(AbsoluteOffset, Update)]]
  )(implicit materializer: Materializer, ec: ExecutionContext): Future[
    (
        Seq[Vector[(AbsoluteOffset, Update)]],
        Seq[Vector[(AbsoluteOffset, Update)]],
        Seq[OffsetCheckpoint],
    )
  ] = {
    val elementsQueue =
      new ConcurrentLinkedQueue[Option[(AbsoluteOffset, Update)]]
    inputSeq.foreach(elementsQueue.add)

    val flattenedSeq: Seq[Vector[(AbsoluteOffset, Update)]] =
      inputSeq.flatten.map(Vector(_))

    val bufferSize = 100
    val (sourceQueueSomes, sourceSomes) = Source
      .queue[Vector[(AbsoluteOffset, Update)]](bufferSize)
      .preMaterialize()
    val (sourceQueueNones, sourceNones) = Source
      .queue[Option[Nothing]](bufferSize)
      .preMaterialize()

    def offerNext() =
      Option(elementsQueue.poll()) match {
        // send element
        case Some(Some(pair)) =>
          sourceQueueSomes.offer(Vector(pair))
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
      .map((_, someLedgerEnd))
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
      .map(_._1)
      .alsoTo(Sink.foreach(_ => offerNext()))
      .runWith(Sink.seq)

    output.map(o => (flattenedSeq, o, checkpoints))

  }

  private def transactionAccepted(t: Long, domainId: DomainId): Update.TransactionAccepted =
    Update.SequencedTransactionAccepted(
      completionInfoO = None,
      transactionMeta = someTransactionMeta,
      transaction = CommittedTransaction(TransactionBuilder.Empty),
      updateId = txId1,
      hostedWitnesses = Nil,
      contractMetadata = Map.empty,
      domainId = domainId,
      requestCounter = RequestCounter(1),
      sequencerCounter = SequencerCounter(1),
      recordTime = CantonTimestamp(Timestamp(t)),
    )

  private def assignmentAccepted(
      t: Long,
      source: DomainId,
      target: DomainId,
  ): Update.ReassignmentAccepted =
    Update.SequencedReassignmentAccepted(
      optCompletionInfo = None,
      workflowId = Some(workflowId),
      updateId = txId3,
      reassignmentInfo = ReassignmentInfo(
        sourceDomain = ReassignmentTag.Source(source),
        targetDomain = ReassignmentTag.Target(target),
        submitter = Option(party1),
        reassignmentCounter = 15L,
        hostedStakeholders = party2 :: Nil,
        unassignId = CantonTimestamp.assertFromLong(155555L),
        isReassigningParticipant = true,
      ),
      reassignment = Reassignment.Assign(
        ledgerEffectiveTime = Timestamp.assertFromLong(12222),
        createNode = someCreateNode,
        contractMetadata = someContractMetadataBytes,
      ),
      requestCounter = RequestCounter(1),
      sequencerCounter = SequencerCounter(1),
      recordTime = CantonTimestamp(Timestamp(t)),
    )

  private def unassignmentAccepted(
      t: Long,
      source: DomainId,
      target: DomainId,
  ): Update.ReassignmentAccepted =
    Update.SequencedReassignmentAccepted(
      optCompletionInfo = None,
      workflowId = Some(workflowId),
      updateId = txId4,
      reassignmentInfo = ReassignmentInfo(
        sourceDomain = ReassignmentTag.Source(source),
        targetDomain = ReassignmentTag.Target(target),
        submitter = Option(party2),
        reassignmentCounter = 15L,
        hostedStakeholders = party1 :: Nil,
        unassignId = CantonTimestamp.assertFromLong(1555551L),
        isReassigningParticipant = true,
      ),
      reassignment = Reassignment.Unassign(
        contractId = someCreateNode.coid,
        templateId = templateId2,
        packageName = packageName,
        stakeholders = List(party2),
        assignmentExclusivity = Some(Timestamp.assertFromLong(123456L)),
      ),
      requestCounter = RequestCounter(1),
      sequencerCounter = SequencerCounter(1),
      recordTime = CantonTimestamp(Timestamp(t)),
    )

  private def commandRejected(t: Long, domainId: DomainId): Update.CommandRejected =
    Update.SequencedCommandRejected(
      completionInfo = CompletionInfo(
        actAs = List.empty,
        applicationId = Ref.ApplicationId.assertFromString("some-app-id"),
        commandId = Ref.CommandId.assertFromString("cmdId"),
        optDeduplicationPeriod = None,
        submissionId = None,
      ),
      reasonTemplate = FinalReason(new Status()),
      domainId = domainId,
      requestCounter = RequestCounter(1),
      sequencerCounter = SequencerCounter(1),
      recordTime = CantonTimestamp.assertFromLong(t),
    )

  private def sequencerIndexMoved(t: Long, domainId: DomainId): Update.SequencerIndexMoved =
    Update.SequencerIndexMoved(
      domainId = domainId,
      sequencerCounter = SequencerCounter(1),
      recordTime = CantonTimestamp.assertFromLong(t),
      requestCounterO = None,
    )

  private val someLedgerEnd = LedgerEnd(
    lastOffset = AbsoluteOffset.tryFromLong(10L),
    lastEventSeqId = 10L,
    lastStringInterningId = 10,
    lastPublicationTime = CantonTimestamp.assertFromLong(10L),
  )

}

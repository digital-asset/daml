// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{Materializer, QueueOfferResult}
import ch.qos.logback.classic.Level
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value.{ContractId, ValueInt64, ValueText, VersionedValue}
import com.daml.logging.LoggingContext
import com.daml.platform.index.BuffersUpdaterSpec.{contractStateEventMock, transactionLogUpdateMock}
import com.daml.platform.store.appendonlydao.events.{Contract, Key, Party}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.testing.LogCollector
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable
import scala.concurrent.duration._

final class BuffersUpdaterSpec
    extends AnyWordSpec
    with Matchers
    with Eventually
    with BeforeAndAfterAll {
  private val actorSystem = ActorSystem("test")
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private implicit val materializer: Materializer = Materializer(actorSystem)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2000, Millis)), interval = scaled(Span(50, Millis)))

  private def readLog(): Seq[(Level, String)] = LogCollector.read[this.type, BuffersUpdater]

  "event stream consumption" should {
    "populate the in-memory buffers using the transaction log updates subscription" in {
      val (queue, source) = Source
        .queue[((Offset, Long), TransactionLogUpdate)](16)
        .preMaterialize()

      val someOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
      val someEventSeqId = 1337L

      val updateMock = transactionLogUpdateMock()
      val contractStateEventMocks = (1 to 3).map(_ => contractStateEventMock())

      val transactionsBufferMock =
        scala.collection.mutable.ArrayBuffer.empty[(Offset, TransactionLogUpdate)]
      val updateTransactionsBufferMock = (offset: Offset, update: TransactionLogUpdate) => {
        transactionsBufferMock += (offset -> update)
        ()
      }
      val contractStateMock = scala.collection.mutable.ArrayBuffer.empty[ContractStateEvent]

      val buffersUpdater = BuffersUpdater(
        subscribeToTransactionLogUpdates = { (_, _) => source },
        updateTransactionsBuffer = updateTransactionsBufferMock,
        toContractStateEvents = Map(updateMock -> contractStateEventMocks.iterator),
        updateMutableCache = contractStateMock += _,
        minBackoffStreamRestart = 10.millis,
        sysExitWithCode = _ => fail("should not be triggered"),
      )(materializer, loggingContext, scala.concurrent.ExecutionContext.global)

      queue.offer((someOffset, someEventSeqId) -> updateMock) shouldBe QueueOfferResult.Enqueued

      eventually {
        transactionsBufferMock should contain theSameElementsAs Seq(someOffset -> updateMock)
        contractStateMock should contain theSameElementsInOrderAs contractStateEventMocks
        buffersUpdater.updaterIndex.get shouldBe (someOffset -> someEventSeqId)
      }
    }
  }

  "transaction log updates subscription" should {
    "restart if the source fails" in {
      val update1, update2, update3 = transactionLogUpdateMock()
      val Seq(offset1, offset2, offset3) =
        (1L to 3L).map(idx => Offset.fromByteArray(BigInt(1337L + idx).toByteArray) -> idx)

      val sourceSubscriptionFixture: (
          Offset,
          EventSequentialId,
      ) => Source[((Offset, Long), TransactionLogUpdate), NotUsed] = {
        case (Offset.beforeBegin, 0L) => // TODO: append-only: FIXME consolidating parameters table
          Source(
            immutable.Iterable(
              offset1 -> update1,
              offset2 -> update2,
              offset3 -> update2, /* Wrong on purpose */
            )
          )
            .map(x =>
              if (x._1._2 == 3L) {
                println(s"been here $x")
                throw new RuntimeException("some transient failure")
              } else x
            )
        case (_, 2L) =>
          Source
            .single(offset3 -> update3)
            // Keep alive so we don't trigger unwanted restarts (for the purpose of this test)
            .concat(Source.never)
        case unexpected => fail(s"Unexpected re-subscription point $unexpected")
      }

      val transactionsBufferMock =
        scala.collection.mutable.ArrayBuffer.empty[(Offset, TransactionLogUpdate)]
      val updateTransactionsBufferMock = (offset: Offset, update: TransactionLogUpdate) => {
        transactionsBufferMock += (offset -> update)
        ()
      }

      val buffersUpdater = BuffersUpdater(
        subscribeToTransactionLogUpdates = sourceSubscriptionFixture,
        updateTransactionsBuffer = updateTransactionsBufferMock,
        toContractStateEvents = Map.empty.withDefaultValue(Iterator.empty),
        updateMutableCache = _ => (),
        minBackoffStreamRestart = 1.millis,
        sysExitWithCode = _ => fail("should not be triggered"),
      )(materializer, loggingContext, scala.concurrent.ExecutionContext.global)

      eventually {
        transactionsBufferMock should contain theSameElementsAs Seq(
          offset1._1 -> update1,
          offset2._1 -> update2,
          offset3._1 -> update3,
        )

        buffersUpdater.updaterIndex.get shouldBe offset3
      }
    }

    "shutdown is triggered with status code 1 if a non-recoverable error occurs in the buffers updating logic" in {
      val update1, update2 = transactionLogUpdateMock()
      val Seq(offset1, offset2) =
        (1L to 2L).map(idx => Offset.fromByteArray(BigInt(1337L + idx).toByteArray) -> idx)

      val sourceSubscriptionFixture = (_: Offset, _: EventSequentialId) =>
        Source(immutable.Iterable(offset1 -> update1, offset2 -> update2))
      val updateTransactionsBufferMock = (offset: Offset, _: TransactionLogUpdate) =>
        if (offset == offset2._1) throw new RuntimeException("Unrecoverable exception")
        else ()

      val shutdownCodeCapture = new AtomicInteger(Integer.MIN_VALUE)

      BuffersUpdater(
        subscribeToTransactionLogUpdates = sourceSubscriptionFixture,
        updateTransactionsBuffer = updateTransactionsBufferMock,
        toContractStateEvents = Map.empty,
        updateMutableCache = _ => (),
        minBackoffStreamRestart = 1.millis,
        sysExitWithCode = shutdownCodeCapture.set,
      )(materializer, loggingContext, scala.concurrent.ExecutionContext.global)

      eventually {
        shutdownCodeCapture.get() shouldBe 1
        val (lastErrorLevel, lastError) = readLog().last
        lastErrorLevel shouldBe Level.ERROR
        lastError should startWith(
          "The transaction log updates stream encountered a non-recoverable error and will shutdown"
        )
      }
    }
  }

  "convertToContractStateEvents" should {
    "convert TransactionLogUpdate.LedgerEndMarker to ContractStateEvent.LedgerEndMarker" in {
      BuffersUpdater
        .convertToContractStateEvents(
          TransactionLogUpdate.LedgerEndMarker(
            eventOffset = Offset.fromByteArray(BigInt(1337L).toByteArray),
            eventSequentialId = 9876L,
          )
        )
        .toVector shouldBe Vector(
        ContractStateEvent.LedgerEndMarker(
          eventOffset = Offset.fromByteArray(BigInt(1337L).toByteArray),
          eventSequentialId = 9876L,
        )
      )
    }

    "convert TransactionLogUpdate.Transaction to a series of ContractStateEvent (created/archived)" in {
      val createdCid = ContractId.assertFromString("#createdCid")
      val createdOffset = Offset.fromByteArray(BigInt(1337L).toByteArray)
      val createdEventSeqId = 9876L
      val createdLedgerEffectiveTime = Instant.ofEpochMilli(987654321L)
      val createdTemplateId = Ref.Identifier.assertFromString("create:template:id")
      val createdContractKey =
        VersionedValue[ContractId](TransactionVersion.VDev, ValueInt64(1337L))
      val createdFlatEventWitnesses = Set("alice", "charlie")
      val createArgument = VersionedValue(TransactionVersion.VDev, ValueText("arg"))
      val createAgreement = "agreement"

      val exercisedCid = ContractId.assertFromString("#exercisedCid")
      val exercisedKey = VersionedValue[ContractId](TransactionVersion.VDev, ValueInt64(8974L))
      val exercisedTemplateId = Ref.Identifier.assertFromString("exercised:template:id")
      val exercisedFlatEventWitnesses = Set("bob", "dan")
      val exercisedOffset = Offset.fromByteArray(BigInt(1337L).toByteArray)
      val exercisedEventSequentialId = 9876L

      val consumingExercise = TransactionLogUpdate.ExercisedEvent(
        contractId = exercisedCid,
        contractKey = Some(exercisedKey),
        templateId = exercisedTemplateId,
        flatEventWitnesses = exercisedFlatEventWitnesses,
        eventOffset = exercisedOffset,
        eventSequentialId = exercisedEventSequentialId,
        consuming = true,
        transactionId = null,
        nodeIndex = 0,
        eventId = null,
        ledgerEffectiveTime = null,
        commandId = null,
        workflowId = null,
        treeEventWitnesses = null,
        submitters = null,
        choice = null,
        actingParties = null,
        children = null,
        exerciseArgument = null,
        exerciseResult = null,
      )
      val createdEvent = TransactionLogUpdate.CreatedEvent(
        contractId = createdCid,
        eventOffset = createdOffset,
        transactionId = null,
        nodeIndex = 0,
        eventSequentialId = createdEventSeqId,
        eventId = null,
        ledgerEffectiveTime = createdLedgerEffectiveTime,
        templateId = createdTemplateId,
        commandId = null,
        workflowId = null,
        contractKey = Some(createdContractKey),
        treeEventWitnesses = Set("bob"), // Unused in ContractStateEvent
        flatEventWitnesses = createdFlatEventWitnesses,
        submitters = null,
        createArgument = createArgument,
        createSignatories = null,
        createObservers = null,
        createAgreementText = Some(createAgreement),
      )
      val transaction = TransactionLogUpdate.Transaction(
        transactionId = "some-tx-id",
        workflowId = "some-workflow-id",
        effectiveAt = Instant.EPOCH,
        offset = Offset.beforeBegin,
        events = Vector(
          createdEvent,
          consumingExercise,
          consumingExercise.copy(
            consuming = false, // Non-consuming exercise should not appear in ContractStateEvents
            eventSequentialId = consumingExercise.eventSequentialId + 1L,
          ),
        ),
      )

      BuffersUpdater
        .convertToContractStateEvents(transaction)
        .toVector should contain theSameElementsInOrderAs Vector(
        ContractStateEvent.Created(
          contractId = createdCid,
          contract = Contract(
            template = createdTemplateId,
            arg = createArgument,
            agreementText = createAgreement,
          ),
          globalKey = Some(Key.assertBuild(createdTemplateId, createdContractKey.value)),
          ledgerEffectiveTime = createdLedgerEffectiveTime,
          stakeholders = createdFlatEventWitnesses.map(Party.assertFromString),
          eventOffset = createdOffset,
          eventSequentialId = createdEventSeqId,
        ),
        ContractStateEvent.Archived(
          contractId = exercisedCid,
          globalKey = Some(Key.assertBuild(exercisedTemplateId, exercisedKey.value)),
          stakeholders = exercisedFlatEventWitnesses.map(Party.assertFromString),
          eventOffset = exercisedOffset,
          eventSequentialId = exercisedEventSequentialId,
        ),
      )
    }
  }

  override def afterAll(): Unit = {
    materializer.shutdown()
    val _ = actorSystem.terminate()
  }
}

private object BuffersUpdaterSpec {
  val mockSeed = new AtomicLong(0L)
  /* We use the simplest embodiments of the interfaces as mocks and pick the eventSequentialId as discriminator */
  private def transactionLogUpdateMock() =
    TransactionLogUpdate.LedgerEndMarker(Offset.beforeBegin, mockSeed.getAndIncrement())
  private def contractStateEventMock() =
    ContractStateEvent.LedgerEndMarker(Offset.beforeBegin, mockSeed.getAndIncrement())
}

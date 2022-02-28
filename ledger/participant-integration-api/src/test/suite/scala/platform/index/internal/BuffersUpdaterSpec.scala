// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index.internal

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import ch.qos.logback.classic.Level
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceContext
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{TransactionVersion, Versioned}
import com.daml.lf.value.Value.{ContractId, ValueInt64, ValueText}
import com.daml.logging.LoggingContext
import com.daml.platform.index.internal.BuffersUpdater.{
  BuffersIndex,
  SubscribeToTransactionLogUpdates,
}
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.appendonlydao.events.{Contract, ContractStateEvent, Key, Party}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.testing.LogCollector
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

import BuffersUpdaterSpec._

final class BuffersUpdaterSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with BeforeAndAfterAll {
  private val actorSystem = ActorSystem("test")
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private implicit val materializer: Materializer = Materializer(actorSystem)
  private implicit val resourceContext: ResourceContext = ResourceContext(
    scala.concurrent.ExecutionContext.global
  )

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(2000, Millis)), interval = scaled(Span(50, Millis)))

  private def readLog(): Seq[(Level, String)] = LogCollector.read[this.type, BuffersUpdater.type]

  "event stream consumption" should {
    "populate the in-memory buffers using the transaction log updates subscription" in {
      val contractStateEventMocks = (1 to 3).map(_ => contractStateEventMock())

      assertWithBuffersUpdater(
        subscribeToTransactionLogUpdates = someUpdatesSourceSubscription,
        toContractStateEvents = Map(someUpdate -> contractStateEventMocks.iterator),
      ) { (updaterIndex, transactionsBuffer, contractState) =>
        eventually {
          Future {
            transactionsBuffer should contain theSameElementsAs Seq(someOffset -> someUpdate)
            contractState should contain theSameElementsInOrderAs contractStateEventMocks
            updaterIndex.get shouldBe Some(someOffset -> someEventSeqId)
          }
        }
      }
    }
  }

  "transaction log updates subscription" should {
    "restart if the source fails" in {
      val updates = (1L to 3L).map { idx =>
        (offset(1337L + idx) -> idx) -> transactionLogUpdateMock()
      }

      val sourceSubscriptionFixture: SubscribeToTransactionLogUpdates = {
        case None =>
          Source(updates).map {
            case ((_, 3), _) => throw new RuntimeException("some transient failure")
            case other => other
          }
        case Some((_, 2L)) =>
          Source
            .single(updates.last)
            // Keep alive so we don't trigger unwanted restarts (for the purpose of this test)
            .concat(Source.never)
        case unexpected => fail(s"Unexpected re-subscription point $unexpected")
      }

      assertWithBuffersUpdater(
        subscribeToTransactionLogUpdates = sourceSubscriptionFixture,
        toContractStateEvents = Map.empty.withDefaultValue(Iterator.empty),
      ) { (updaterIndex, transactionsBuffer, _) =>
        eventually {
          Future {
            transactionsBuffer should contain theSameElementsAs updates.map {
              case ((offset, _), update) => offset -> update
            }

            updaterIndex.get shouldBe Some(updates.last._1)
          }
        }
      }
    }

    "shutdown is triggered with status code 1 if a non-recoverable error occurs in the buffers updating logic" in {
      val shutdownCodeCapture = new AtomicInteger(Integer.MIN_VALUE)
      val failure = "Unrecoverable exception"

      assertWithBuffersUpdater(
        subscribeToTransactionLogUpdates = someUpdatesSourceSubscription,
        toContractStateEvents = { _ => throw new RuntimeException(failure) },
        sysExitWithCode = shutdownCodeCapture.set,
      ) { (_, _, _) =>
        eventually {
          Future {
            shutdownCodeCapture.get() shouldBe 1
            val (lastErrorLevel, lastError) = readLog().last
            lastErrorLevel shouldBe Level.ERROR
            lastError should startWith(
              "The buffers updater stream encountered a non-recoverable error and will shutdown"
            )
          }
        }
      }.recoverWith {
        case NonFatal(e) if e.getMessage.contains(failure) => Future.successful(succeed)
        case other => Future.failed[Assertion](other)
      }
    }
  }

  "convertToContractStateEvents" should {
    "convert TransactionLogUpdate.LedgerEndMarker to ContractStateEvent.LedgerEndMarker" in {
      val conversionResult = BuffersUpdater
        .convertToContractStateEvents(
          TransactionLogUpdate.LedgerEndMarker(someOffset, someEventSeqId)
        )
        .toVector

      conversionResult should contain theSameElementsAs Seq(
        ContractStateEvent.LedgerEndMarker(someOffset, someEventSeqId)
      )
    }

    "convert TransactionLogUpdate.Transaction to a series of ContractStateEvent (created/archived)" in {
      val createdCid = ContractId.V1(Hash.hashPrivateKey("createdCid"))
      val createdOffset = someOffset
      val createdEventSeqId = 9876L
      val createdLedgerEffectiveTime = Timestamp.assertFromLong(987654321L)
      val createdTemplateId = Ref.Identifier.assertFromString("create:template:id")
      val createdContractKey =
        Versioned(TransactionVersion.VDev, ValueInt64(1337L))
      val createdFlatEventWitnesses = Set("alice", "charlie")
      val createArgument = Versioned(TransactionVersion.VDev, ValueText("arg"))
      val createAgreement = "agreement"

      val exercisedCid = ContractId.V1(Hash.hashPrivateKey("exercisedCid"))
      val exercisedKey = Versioned(TransactionVersion.VDev, ValueInt64(8974L))
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
        effectiveAt = Timestamp.Epoch,
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

      val contractStateEvents = BuffersUpdater
        .convertToContractStateEvents(transaction)
        .toVector

      contractStateEvents should contain theSameElementsInOrderAs Vector(
        ContractStateEvent.Created(
          contractId = createdCid,
          contract = Contract(
            template = createdTemplateId,
            arg = createArgument,
            agreementText = createAgreement,
          ),
          globalKey = Some(Key.assertBuild(createdTemplateId, createdContractKey.unversioned)),
          ledgerEffectiveTime = createdLedgerEffectiveTime,
          stakeholders = createdFlatEventWitnesses.map(Party.assertFromString),
          eventOffset = createdOffset,
          eventSequentialId = createdEventSeqId,
        ),
        ContractStateEvent.Archived(
          contractId = exercisedCid,
          globalKey = Some(Key.assertBuild(exercisedTemplateId, exercisedKey.unversioned)),
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

  private def assertWithBuffersUpdater(
      subscribeToTransactionLogUpdates: SubscribeToTransactionLogUpdates,
      toContractStateEvents: TransactionLogUpdate => Iterator[ContractStateEvent],
      minBackoffStreamRestart: FiniteDuration = 1.millis,
      sysExitWithCode: Int => Unit = _ => fail("should not be triggered"),
  )(
      test: (
          BuffersIndex,
          mutable.ArrayBuffer[(Offset, TransactionLogUpdate)],
          mutable.ArrayBuffer[ContractStateEvent],
      ) => Future[Assertion]
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
      resourceContext: ResourceContext,
  ): Future[Assertion] = {
    val transactionsBufferMock = mutable.ArrayBuffer.empty[(Offset, TransactionLogUpdate)]
    val contractStateMock = mutable.ArrayBuffer.empty[ContractStateEvent]

    BuffersUpdater
      .owner(
        subscribeToTransactionLogUpdates = subscribeToTransactionLogUpdates,
        updateTransactionsBuffer = (o, u) => transactionsBufferMock += o -> u,
        updateMutableCache = contractStateMock += _,
        toContractStateEvents = toContractStateEvents,
        minBackoffStreamRestart = minBackoffStreamRestart,
        sysExitWithCode = sysExitWithCode,
      )(mat, loggingContext)
      .use(test(_, transactionsBufferMock, contractStateMock))
  }
}

private object BuffersUpdaterSpec {
  private val mockSeed = new AtomicLong(EventSequentialId.beforeBegin)
  private val someOffset = Offset.fromHexString(Ref.HexString.assertFromString("abcdef"))
  private val someEventSeqId = 1337L
  private val someUpdate = transactionLogUpdateMock()
  private val someUpdatesSourceSubscription: SubscribeToTransactionLogUpdates = _ =>
    Source.single((someOffset, someEventSeqId) -> someUpdate).concat(Source.never)

  private def offset(idx: Long) = Offset.fromByteArray(BigInt(1337L + idx).toByteArray)

  /* We use the simplest embodiments of the interfaces as mocks and pick the eventSequentialId as discriminator */
  private def transactionLogUpdateMock() =
    TransactionLogUpdate.LedgerEndMarker(Offset.beforeBegin, mockSeed.getAndIncrement())
  private def contractStateEventMock() =
    ContractStateEvent.LedgerEndMarker(Offset.beforeBegin, mockSeed.getAndIncrement())
}

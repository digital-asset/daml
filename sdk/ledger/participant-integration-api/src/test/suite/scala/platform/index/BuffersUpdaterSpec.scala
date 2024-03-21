// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{Materializer, QueueOfferResult}
import ch.qos.logback.classic.Level
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.TestResourceContext
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{TransactionVersion, Versioned}
import com.daml.lf.value.Value.{ContractId, ValueInt64, ValueText}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.index.BuffersUpdaterSpec.{contractStateEventMock, transactionLogUpdateMock}
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.testing.LogCollector
import com.daml.platform.{Contract, Key, Party}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

final class BuffersUpdaterSpec
    extends AsyncWordSpec
    with Matchers
    with Eventually
    with TestResourceContext
    with BeforeAndAfterAll {
  private val actorSystem = ActorSystem("test")
  private val metrics = new Metrics(new MetricRegistry)
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

      BuffersUpdater
        .owner(
          subscribeToTransactionLogUpdates = { _ => source },
          updateTransactionsBuffer = updateTransactionsBufferMock,
          toContractStateEvents = Map(updateMock -> contractStateEventMocks.iterator),
          updateMutableCache = contractStateMock ++= _,
          metrics = metrics,
          minBackoffStreamRestart = 10.millis,
          sysExitWithCode = _ => fail("should not be triggered"),
        )(materializer, loggingContext)
        .use { buffersUpdater =>
          queue.offer((someOffset, someEventSeqId) -> updateMock) shouldBe QueueOfferResult.Enqueued

          eventually {
            Future {
              transactionsBufferMock should contain theSameElementsAs Seq(someOffset -> updateMock)
              contractStateMock should contain theSameElementsInOrderAs contractStateEventMocks
              buffersUpdater.updaterIndex.get shouldBe Some((someOffset -> someEventSeqId))
            }
          }
        }
    }
  }

  "transaction log updates subscription" should {
    "restart if the source fails" in {
      val update1, update2, update3 = transactionLogUpdateMock()
      val Seq(offset1, offset2, offset3) =
        (1L to 3L).map(idx => Offset.fromByteArray(BigInt(1337L + idx).toByteArray) -> idx)

      val sourceSubscriptionFixture: Option[
        (
            Offset,
            EventSequentialId,
        )
      ] => Source[((Offset, Long), TransactionLogUpdate), NotUsed] = {
        case None =>
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
        case Some((_, 2L)) =>
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

      BuffersUpdater
        .owner(
          subscribeToTransactionLogUpdates = sourceSubscriptionFixture,
          updateTransactionsBuffer = updateTransactionsBufferMock,
          toContractStateEvents = Map.empty.withDefaultValue(Iterator.empty),
          updateMutableCache = _ => (),
          metrics = metrics,
          minBackoffStreamRestart = 1.millis,
          sysExitWithCode = _ => fail("should not be triggered"),
        )(materializer, loggingContext)
        .use { buffersUpdater =>
          eventually {
            Future {
              transactionsBufferMock should contain theSameElementsAs Seq(
                offset1._1 -> update1,
                offset2._1 -> update2,
                offset3._1 -> update3,
              )

              buffersUpdater.updaterIndex.get shouldBe Some(offset3)
            }
          }
        }
    }

    "shutdown is triggered with status code 1 if a non-recoverable error occurs in the buffers updating logic" in {
      val update1, update2 = transactionLogUpdateMock()
      val Seq(offset1, offset2) =
        (1L to 2L).map(idx => Offset.fromByteArray(BigInt(1337L + idx).toByteArray) -> idx)

      val updateTransactionsBufferMock = (offset: Offset, _: TransactionLogUpdate) =>
        if (offset == offset2._1) throw new RuntimeException("Unrecoverable exception")
        else ()

      val shutdownCodeCapture = new AtomicInteger(Integer.MIN_VALUE)

      BuffersUpdater
        .owner(
          subscribeToTransactionLogUpdates =
            _ => Source(scala.collection.immutable.Seq(offset1 -> update1, offset2 -> update2)),
          updateTransactionsBuffer = updateTransactionsBufferMock,
          toContractStateEvents = Map.empty,
          updateMutableCache = _ => (),
          metrics = metrics,
          minBackoffStreamRestart = 1.millis,
          sysExitWithCode = shutdownCodeCapture.set,
        )(materializer, loggingContext)
        .use { _ =>
          eventually {
            Future {
              shutdownCodeCapture.get() shouldBe 1
              val (lastErrorLevel, lastError) = readLog().last
              lastErrorLevel shouldBe Level.ERROR
              lastError should startWith(
                "The transaction log updates stream encountered a non-recoverable error and will shutdown"
              )
            }
          }
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
      val createdCid = ContractId.V1(Hash.hashPrivateKey("createdCid"))
      val createdOffset = Offset.fromByteArray(BigInt(1337L).toByteArray)
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
      val exercisedFlatEventWitnesses = Set("bob", "dan").map(Ref.Party.assertFromString)
      val exercisedOffset = Offset.fromByteArray(BigInt(1337L).toByteArray)
      val exercisedEventSequentialId = 9876L

      val consumingExercise = TransactionLogUpdate.ExercisedEvent(
        contractId = exercisedCid,
        contractKey = Some(exercisedKey),
        templateId = exercisedTemplateId,
        interfaceId = None,
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
        treeEventWitnesses = Set(Ref.Party.assertFromString("bob")), // Unused in ContractStateEvent
        flatEventWitnesses = createdFlatEventWitnesses.map(Ref.Party.assertFromString),
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
}

private object BuffersUpdaterSpec {
  val mockSeed = new AtomicLong(EventSequentialId.beforeBegin)
  /* We use the simplest embodiments of the interfaces as mocks and pick the eventSequentialId as discriminator */
  private def transactionLogUpdateMock() =
    TransactionLogUpdate.LedgerEndMarker(Offset.beforeBegin, mockSeed.getAndIncrement())
  private def contractStateEventMock() =
    ContractStateEvent.LedgerEndMarker(Offset.beforeBegin, mockSeed.getAndIncrement())
}

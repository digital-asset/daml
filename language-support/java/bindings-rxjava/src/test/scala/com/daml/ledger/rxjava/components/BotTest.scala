// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components

import java.time.{Duration, Instant}
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Collections, Optional, function}

import com.daml.grpc.{GrpcException, GrpcStatus}
import com.daml.ledger.api.auth.AuthServiceWildcard
import com.daml.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse,
}
import com.daml.ledger.api.{v1 => scalaAPI}
import com.daml.ledger.javaapi.data.{Unit => DamlUnit, _}
import com.daml.ledger.rxjava.components.BotTest._
import com.daml.ledger.rxjava.components.LedgerViewFlowable.LedgerView
import com.daml.ledger.rxjava.components.helpers.{CommandsAndPendingSet, CreatedContract}
import com.daml.ledger.rxjava.components.tests.helpers.DummyLedgerClient
import com.daml.ledger.rxjava.grpc.helpers.{LedgerServices, TransactionsServiceImpl}
import com.daml.ledger.rxjava.{CommandSubmissionClient, DamlLedgerClient, untestedEndpoint}
import com.google.protobuf.empty.Empty
import com.google.protobuf.{Empty => JEmpty}
import com.google.rpc.Status
import com.google.rpc.code.Code.OK
import io.grpc.Metadata
import io.reactivex.disposables.Disposable
import io.reactivex.{Flowable, Observable, Single}
import org.pcollections.{HashTreePMap, HashTreePSet}
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

final class BotTest extends AnyFlatSpec with Matchers with Eventually {
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 1.second)

  private def ledgerServices: LedgerServices = new LedgerServices("bot-test")

  "Bot" should "create a flowable of WorkflowEvents from the ACS" in {
    val acs = new TestFlowable[GetActiveContractsResponse]("acs")
    val transactions = new TestFlowable[Transaction]("transaction")
    val templateId = new Identifier("pid", "mname", "ename")
    val ledgerClient = new DummyLedgerClient(
      "ledgerId",
      acs,
      Flowable.empty(),
      transactions,
      Flowable.empty(),
      new LedgerOffset.Absolute("0"),
    )
    val filter = new FiltersByParty(
      Map("Alice" -> new InclusiveFilter(Set(templateId).asJava).asInstanceOf[Filter]).asJava
    )

    val workflowEvents = Bot.activeContractSetAndNewTransactions(ledgerClient, filter)
    val secondElementOffset = 1
    val contract1 = create("Alice", templateId)
    val acsEvent1 =
      new GetActiveContractsResponse(secondElementOffset.toString, List(contract1).asJava, "wid")
    val testSub = workflowEvents.test()

    acs.emit(acsEvent1)
    acs.complete()
    transactions.complete()

    testSub
      .assertComplete()
      .assertResult(acsEvent1)
  }

  it should "create a flowable of WorkflowEvents from the transactions" in {
    val transactions = new TestFlowable[Transaction]("transactions")
    val templateId = new Identifier("pid", "mname", "ename")
    val ledgerClient = new DummyLedgerClient(
      "ledgerId",
      Flowable.empty(),
      Flowable.empty(),
      transactions,
      Flowable.empty(),
      new LedgerOffset.Absolute("0"),
    )
    val filter = new FiltersByParty(
      Map("Alice" -> new InclusiveFilter(Set(templateId).asJava).asInstanceOf[Filter]).asJava
    )

    val workflowEvents = Bot.activeContractSetAndNewTransactions(ledgerClient, filter)
    val contract1 = create("Alice", templateId)
    val t1 = new Transaction(
      "transactionId",
      "commandId",
      "workflowId",
      ZeroTimestamp,
      List[Event](contract1).asJava,
      "1",
    )
    val testSub = workflowEvents.test()

    transactions.emit(t1)
    transactions.complete()

    testSub
      .assertComplete()
      .assertResult(t1)
  }

  it should "handle submission failures non-fatally" in {

    val appId = "test-app-id"
    val ledgerId = "test-ledger-id"
    val templateId = new Identifier("pid", "mname", "ename")
    val party = "Alice"

    val transactions = new TestFlowable[Transaction]("transactions")
    val ledgerClient =
      new DummyLedgerClient(
        ledgerId,
        Flowable.empty(),
        Flowable.empty(),
        transactions,
        Flowable.never(),
        new LedgerOffset.Absolute("0"),
      ) {
        override def getCommandSubmissionClient: CommandSubmissionClient =
          new CommandSubmissionClient {

            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                party: String,
                minLedgerTimeAbs: Optional[Instant],
                minLedgerTimeRel: Optional[Duration],
                deduplicationTime: Optional[Duration],
                commands: util.List[Command],
            ): Single[JEmpty] = {
              submitted.append(
                new SubmitCommandsRequest(
                  workflowId,
                  applicationId,
                  commandId,
                  party,
                  minLedgerTimeAbs,
                  minLedgerTimeRel,
                  deduplicationTime,
                  commands,
                )
              )
              Single.error(new RuntimeException("expected failure"))
            }

            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                actAs: java.util.List[String],
                readAS: java.util.List[String],
                minLedgerTimeAbs: Optional[Instant],
                minLedgerTimeRel: Optional[Duration],
                deduplicationTime: Optional[Duration],
                commands: util.List[Command],
            ): Single[JEmpty] =
              untestedEndpoint

            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                party: String,
                minLedgerTimeAbs: Optional[Instant],
                minLedgerTimeRel: Optional[Duration],
                deduplicationTime: Optional[Duration],
                commands: util.List[Command],
                accessToken: String,
            ): Single[JEmpty] =
              untestedEndpoint

            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                actAs: java.util.List[String],
                readAS: java.util.List[String],
                minLedgerTimeAbs: Optional[Instant],
                minLedgerTimeRel: Optional[Duration],
                deduplicationTime: Optional[Duration],
                commands: util.List[Command],
                accessToken: String,
            ): Single[JEmpty] =
              untestedEndpoint

            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                party: String,
                commands: util.List[Command],
            ): Single[JEmpty] =
              untestedEndpoint

            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                actAs: java.util.List[String],
                readAS: java.util.List[String],
                commands: util.List[Command],
            ): Single[JEmpty] =
              untestedEndpoint

            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                party: String,
                commands: util.List[Command],
                accessToken: String,
            ): Single[JEmpty] =
              untestedEndpoint

            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                actAs: java.util.List[String],
                readAS: java.util.List[String],
                commands: util.List[Command],
                accessToken: String,
            ): Single[JEmpty] =
              untestedEndpoint
          }
      }

    val parties = new java.util.HashMap[String, Filter]()
    parties.put(party, new InclusiveFilter(Set(templateId).asJava).asInstanceOf[Filter])
    val transactionFilter = new FiltersByParty(parties)

    val finishedWork = new AtomicBoolean(false)

    val cid = "cid_1"
    val bot: function.Function[LedgerView[AtomicInteger], Flowable[CommandsAndPendingSet]] =
      new function.Function[LedgerView[AtomicInteger], Flowable[CommandsAndPendingSet]] {
        private val logger = LoggerFactory.getLogger("TestBot")

        override def apply(fcs: LedgerView[AtomicInteger]): Flowable[CommandsAndPendingSet] = {
          logger.debug(s"apply(fcs: $fcs)")
          val contracts = fcs.getContracts(templateId)
          if (contracts.isEmpty) {
            // this function gets called again after the contract has been set to pending, so we
            // expect no free contract in the ledger view
            Flowable.empty()
          } else {
            // after each failure, we expect to see the same initial contract again
            assert(contracts.size() == 1, "contracts != 1")
            val counter = contracts.get(cid)

            // let's run this test 3 times
            if (counter.get() < 3) {
              counter.incrementAndGet()
              val command = new ExerciseCommand(templateId, cid, "choice", DamlUnit.getInstance())
              val commandList = List[Command](command).asJava
              val commands = new SubmitCommandsRequest(
                "wid",
                appId,
                s"commandId_${counter.get()}",
                party,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                commandList,
              )
              Flowable.fromArray(
                new CommandsAndPendingSet(
                  commands,
                  HashTreePMap.singleton(templateId, HashTreePSet.singleton(cid)),
                )
              )
            } else {
              finishedWork.set(true)
              Flowable.empty()
            }
          }
        }
      }

    val counter = new AtomicInteger(0)
    using(Bot.wire(appId, ledgerClient, transactionFilter, bot, _ => counter)) {
      // when the bot is wired-up, no command should have been submitted to the server
      ledgerClient.submitted should have size 0
      counter.get shouldBe 0

      // when the bot receives a transaction, a command should be submitted to the server
      val createdEvent1 = create(party, templateId, id = 1)
      transactions.emit(transaction(createdEvent1))

      eventually {
        finishedWork.get shouldBe true
      }

      ledgerClient.submitted should have size 3
      counter.get shouldBe 3

      transactions.complete()
    }
  }

  it should "wire a bot to the ledger-client" in {
    val appId = "test-app-id"
    val ledgerId = "test-ledger-id"
    val templateId = new Identifier("pid", "mname", "ename")
    val party = "Alice"

    val transactions = new TestFlowable[Transaction]("transactions")
    val commandCompletions = new TestFlowable[CompletionStreamResponse]("completions")
    val ledgerClient =
      new DummyLedgerClient(
        ledgerId,
        Flowable.empty(),
        Flowable.empty(),
        transactions,
        commandCompletions,
        new LedgerOffset.Absolute("0"),
      )

    val parties = new java.util.HashMap[String, Filter]()
    parties.put(party, new InclusiveFilter(Set(templateId).asJava).asInstanceOf[Filter])
    val transactionFilter = new FiltersByParty(parties)

    // the "mirror" bot is a bot that for each contract found emits one exercise command.
    // In this way we can see how many commands are submitted to the ledger-client and
    // verify the result
    val atomicCount = new AtomicInteger(0)
    val bot: function.Function[LedgerView[CreatedContract], Flowable[CommandsAndPendingSet]] =
      new function.Function[LedgerView[CreatedContract], Flowable[CommandsAndPendingSet]] {

        private val logger = LoggerFactory.getLogger("TestBot")

        override def apply(fcs: LedgerView[CreatedContract]): Flowable[CommandsAndPendingSet] = {
          logger.debug(s"apply(fcs: $fcs)")
          Flowable.fromIterable(
            fcs
              .getContracts(templateId)
              .asScala
              .map { case (contractId, _) =>
                val command =
                  new ExerciseCommand(templateId, contractId, "choice", DamlUnit.getInstance())
                logger.debug(s"exercise: $command")
                val commandList = List[Command](command).asJava
                val commands =
                  new SubmitCommandsRequest(
                    "wid",
                    appId,
                    s"commandId_${atomicCount.incrementAndGet()}",
                    party,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    commandList,
                  )
                logger.debug(s"commands: $commands")
                new CommandsAndPendingSet(
                  commands,
                  HashTreePMap.singleton(templateId, HashTreePSet.singleton(contractId)),
                )
              }
              .asJava
          )
        }
      }
    using(Bot.wireSimple(appId, ledgerClient, transactionFilter, bot)) {
      // when the bot is wired-up, no command should have been submitted to the server
      eventually {
        ledgerClient.submitted should have size 0
      }

      // when the bot receives a transaction, a command should be submitted to the server
      val createdEvent1 = create(party, templateId)
      transactions.emit(transaction(createdEvent1))
      eventually {
        ledgerClient.submitted should have size 1
      }

      val archivedEvent1 = archive(createdEvent1)
      val createEvent2 = create(party, templateId)
      val createEvent3 = create(party, templateId)
      transactions.emit(transaction(archivedEvent1, createEvent2, createEvent3))
      eventually {
        ledgerClient.submitted should have size 3
      }

      // we complete the first command with success and then check that the client hasn't submitted a new command
      commandCompletions.emit(
        new CompletionStreamResponse(
          Optional.of(new Checkpoint(ZeroTimestamp, new LedgerOffset.Absolute(""))),
          List(
            scalaAPI.CompletionOuterClass.Completion
              .newBuilder()
              .setCommandId("commandId_0")
              .setStatus(Status.newBuilder().setCode(OK.value).build())
              .build()
          ).asJava,
        )
      )
      Thread.sleep(100)
      ledgerClient.submitted should have size 3

      // WARNING: THE FOLLOWING TEST IS NOT PASSING YET
      // // we complete the second command with failure and then check that the client has submitted a new command
      // commandCompletions.emit(
      //   new CompletionStreamResponse(
      //     Optional.of(new Checkpoint(ZeroTimestamp, new LedgerOffset.Absolute(""))),
      //     List(
      //       CompletionOuterClass.Completion
      //         .newBuilder()
      //         .setCommandId("commandId_1")
      //         .setStatus(Status.newBuilder().setCode(INVALID_ARGUMENT.value))
      //         .build(),
      //     ).asJava
      //   ))
      // eventually {
      //   ledgerClient.submitted should have size 4
      // }

      transactions.complete()
      commandCompletions.complete()
    }
  }

  it should "query first the ACS and then the LedgerEnd sequentially so that there is not race condition and LedgerEnd >= ACS offset" in {

    /** This tests that the Bot has finished dealing with the ACS before asking for the LedgerEnd.
      * The reason why this is essential is that the ACS can change in between the LedgerEnd is obtained and the ACS
      * is exhausted, leading to the offset of the ACS being after the LedgerEnd. If this happens, the Bot will ask
      * for transactions to the Ledger with begin offset bigger than end offset and the Ledger will return an error that
      * blocks the initialization of the Bot.
      *
      * The tests fabricates special ACS and Transaction streams such that the Transaction streams has the correct
      * offset only if the ACS stream has been exhausted. To do so, it uses a [[Promise]] called `getACSDone` which
      * will be fulfilled when the ACS stream is completed. This promise is then used to define the transactions
      * stream based on if it's fulfilled or not: if fulfilled, the last transaction will contain the same offset
      * of the ACS, otherwise it will contain an offset before it.
      *
      * Note that the tests delay sending the ACS to the Bot in order to "guarantee" that the test is not green because
      * of race conditions.
      */
    val (wrongOffset, rightOffset) = ("0", "1")

    /* The promise that will be fulfilled when the ACS stream completes */
    val getACSDone = Promise[Unit]()

    /* The ACS stream delayed by 100 milliseconds and when completed fulfills the promise `getACSDone` */
    val getActiveContractsResponse =
      new scalaAPI.active_contracts_service.GetActiveContractsResponse(rightOffset)
    val getActiveContractsResponses = Observable
      .fromArray(getActiveContractsResponse)
      .delay(100, TimeUnit.MILLISECONDS)
      .doOnComplete(() => getACSDone.success(()))

    /*The Transaction stream is deferred and created only when requested. The creation of the stream depends on the value
     * of getACSDone.future.isCompleted
     */
    def dummyTransactionResponse(offset: String) =
      TransactionsServiceImpl.LedgerItem(
        "",
        "",
        "",
        com.google.protobuf.timestamp.Timestamp.defaultInstance,
        Seq(),
        offset,
      )
    val getTransactionsResponses = Observable.defer[TransactionsServiceImpl.LedgerItem](() =>
      Observable.fromArray(
        dummyTransactionResponse(if (getACSDone.future.isCompleted) rightOffset else wrongOffset)
      )
    )

    ledgerServices.withFakeLedgerServer(
      getActiveContractsResponses,
      getTransactionsResponses,
      Future.successful(com.google.protobuf.empty.Empty.defaultInstance),
      List.empty,
      scalaAPI.command_completion_service.CompletionEndResponse.defaultInstance,
      Future.successful(Empty.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionIdResponse.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionResponse.defaultInstance),
      Future.successful(SubmitAndWaitForTransactionTreeResponse.defaultInstance),
      List.empty,
      Seq.empty,
      Future.successful(scalaAPI.package_service.ListPackagesResponse.defaultInstance),
      Future.successful(scalaAPI.package_service.GetPackageResponse.defaultInstance),
      Future.successful(scalaAPI.package_service.GetPackageStatusResponse.defaultInstance),
      AuthServiceWildcard,
    ) { (server, _) =>
      val client =
        DamlLedgerClient.newBuilder("localhost", server.getPort).build()
      client.connect()

      /* The bot is wired here and inside wire is where the race condition can happen. We catch the possible
       * error to pretty-print it. This try-catch depends on the implementation of `TransactionServiceImpl.getTransactionTree()`
       */
      try {
        Bot
          .wireSimple(
            "appId",
            client,
            new FiltersByParty(Collections.emptyMap()),
            _ => Flowable.empty(),
          )
          .dispose()
      } catch {
        case GrpcException(GrpcStatus.INVALID_ARGUMENT(), trailers) =>
          /** the tests relies on specific implementation of the [[TransactionsServiceImpl.getTransactions()]] */
          fail(trailers.get(Metadata.Key.of("cause", Metadata.ASCII_STRING_MARSHALLER)))
      }

      // If there is an exception, we just ignore it as there are some problems with gRPC.
      try {
        client.close()
      } catch {
        case NonFatal(exception) =>
          logger.warn(
            "Closing DamlLedgerClient caused an error, ignoring it because it can happen and it should not be a problem",
            exception,
          )
      }
    }
  }
}

object BotTest {
  private val ZeroTimestamp = Instant.EPOCH

  private val logger = LoggerFactory.getLogger(classOf[BotTest])
  private val random = new Random()

  private def create(
      party: String,
      templateId: Identifier,
      id: Int = random.nextInt(),
  ): CreatedEvent =
    new CreatedEvent(
      List(party).asJava,
      s"eid_$id",
      templateId,
      s"cid_$id",
      new DamlRecord(List.empty[DamlRecord.Field].asJava),
      Optional.empty(),
      Optional.empty(),
      Collections.emptySet(),
      Collections.emptySet(),
    )

  private def archive(event: CreatedEvent): ArchivedEvent =
    new ArchivedEvent(
      event.getWitnessParties,
      s"${event.getEventId}_archive",
      event.getTemplateId,
      event.getContractId,
    )

  private def transaction(events: Event*): Transaction =
    new Transaction(
      "tid",
      s"cid_${random.nextInt()}",
      "wid",
      ZeroTimestamp,
      events.toList.asJava,
      events.toList.size.toString,
    )

  private class TestFlowable[A](name: String) extends Flowable[A] {
    private val logger = LoggerFactory.getLogger(s"${getClass.getSimpleName}($name)")

    private val buffer: mutable.Buffer[A] = mutable.Buffer.empty[A]
    private var observerMay = Option.empty[Subscriber[_ >: A]]
    private var isComplete = false

    override def subscribeActual(observer: Subscriber[_ >: A]): Unit = {
      observerMay match {
        case Some(oldObserver) =>
          throw new IllegalStateException(
            s"${getClass.getSimpleName} supports only one subscriber. Currently subscribed $oldObserver, want to subscribe $observer"
          )
        case None =>
          observerMay = Option(observer)
          logger.debug(s"Subscribed observer $observer")
          observer.onSubscribe(new Subscription {
            override def request(n: Long): Unit = ()

            override def cancel(): Unit = ()
          })
          buffer.foreach(a => observer.onNext(a))
          buffer.clear()
          if (isComplete) {
            observer.onComplete()
          }
      }
    }

    def emit(a: A): Unit = {
      observerMay match {
        case None =>
          logger.debug(s"no observer, buffering $a")
          buffer.append(a)
        case Some(observer) =>
          logger.debug(s"emitting $a to subscribed $observer")
          observer.onNext(a)
      }
    }

    def complete(): Unit = {
      observerMay match {
        case None =>
          logger.debug("no observer, buffering onComplete")
          isComplete = true
        case Some(observer) =>
          logger.debug(s"calling onComplete() on subscribed $observer")
          observer.onComplete()
      }
    }
  }

  private def using[A <: Disposable, B](disposable: A)(run: => B): B = {
    try {
      run
    } finally {
      disposable.dispose()
    }
  }
}

// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.rxjava.components

import java.time.Instant
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Collections, Optional}

import com.daml.ledger.javaapi.data.{Unit => DAMLUnit, _}
import com.daml.ledger.rxjava.components.LedgerViewFlowable.LedgerView
import com.daml.ledger.rxjava.components.helpers.{CommandsAndPendingSet, CreatedContract}
import com.daml.ledger.rxjava.components.tests.helpers.DummyLedgerClient
import com.daml.ledger.rxjava.grpc.helpers.{DataLayerHelpers, LedgerServices}
import com.daml.ledger.rxjava.{CommandSubmissionClient, DamlLedgerClient}
import com.daml.ledger.testkit.services.TransactionServiceImpl
import com.digitalasset.ledger.api.v1.command_service.{
  SubmitAndWaitForTransactionIdResponse,
  SubmitAndWaitForTransactionResponse,
  SubmitAndWaitForTransactionTreeResponse
}
import com.digitalasset.ledger.api.{v1 => scalaAPI}
import com.google.protobuf.{Empty => JEmpty}
import com.google.protobuf.empty.Empty
import com.google.rpc.Status
import io.grpc.Metadata
import io.grpc.Status.Code
import io.reactivex.{Flowable, Observable, Single}
import org.pcollections.{HashTreePMap, HashTreePSet}
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random
import scala.util.control.NonFatal

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class BotTest extends FlatSpec with Matchers with DataLayerHelpers {

  override def ledgerServices: LedgerServices = new LedgerServices("bot-test")
  val ec: ExecutionContext = ledgerServices.executionContext
  val logger = LoggerFactory.getLogger(this.getClass)

  val random = new Random()
  val zeroTimestamp = Instant.EPOCH

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
      new LedgerOffset.Absolute("0"))
    val filter = new FiltersByParty(
      Map("Alice" -> new InclusiveFilter(Set(templateId).asJava).asInstanceOf[Filter]).asJava)

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
//    val acs = new TestFlowable[GetActiveContractsResponse]("acs")
    val transactions = new TestFlowable[Transaction]("transactions")
    val templateId = new Identifier("pid", "mname", "ename")
    val ledgerClient = new DummyLedgerClient(
      "ledgerId",
      Flowable.empty(),
      Flowable.empty(),
      transactions,
      Flowable.empty(),
      new LedgerOffset.Absolute("0"))
    val filter = new FiltersByParty(
      Map("Alice" -> new InclusiveFilter(Set(templateId).asJava).asInstanceOf[Filter]).asJava)

    val workflowEvents = Bot.activeContractSetAndNewTransactions(ledgerClient, filter)
    val contract1 = create("Alice", templateId)
    val t1 = new Transaction(
      "transactionId",
      "commandId",
      "workflowId",
      zeroTimestamp,
      List[Event](contract1).asJava,
      "1")
    val testSub = workflowEvents.test()

    transactions.emit(t1)
//    acs.complete()
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
        new LedgerOffset.Absolute("0")) {
        override def getCommandSubmissionClient: CommandSubmissionClient =
          new CommandSubmissionClient {
            override def submit(
                workflowId: String,
                applicationId: String,
                commandId: String,
                party: String,
                ledgerEffectiveTime: Instant,
                maximumRecordTime: Instant,
                commands: util.List[Command]): Single[JEmpty] = {
              submitted.append(
                new SubmitCommandsRequest(
                  workflowId,
                  applicationId,
                  commandId,
                  party,
                  ledgerEffectiveTime,
                  maximumRecordTime,
                  commands))
              Single.error(new RuntimeException("expected failure"))
            }
          }
      }

    val parties = new java.util.HashMap[String, Filter]()
    parties.put(party, new InclusiveFilter(Set(templateId).asJava).asInstanceOf[Filter])
    val transactionFilter = new FiltersByParty(parties)

    val finishedWork = new AtomicBoolean(false)

    val cid = "cid_1"
    val bot =
      new java.util.function.Function[LedgerView[AtomicInteger], Flowable[CommandsAndPendingSet]] {

        private val logger = LoggerFactory.getLogger("TestBot")

        override def apply(fcs: LedgerView[AtomicInteger]): Flowable[CommandsAndPendingSet] = {
          logger.debug(s"apply(fcs: $fcs)")
          val contracts = fcs.getContracts(templateId)
          if (contracts.isEmpty) {
            // this function gets called again after the contract has been set to pending, so we expect no free contract
            // in the ledger view
            Flowable.empty()
          } else {
            // after each failure, we expect to see the same initial contract again
            assert(contracts.size() == 1, "contracts != 1")
            val counter = contracts.get(cid)

            // let's run this test 3 times
            if (counter.get() < 3) {
              counter.incrementAndGet()
              val command = new ExerciseCommand(templateId, cid, "choice", DAMLUnit.getInstance())
              val commandList = List[Command](command).asJava
              val commands = new SubmitCommandsRequest(
                "wid",
                appId,
                s"commandId_${counter.get()}",
                party,
                zeroTimestamp,
                zeroTimestamp,
                commandList
              )
              Flowable.fromArray(
                new CommandsAndPendingSet(
                  commands,
                  HashTreePMap.singleton(templateId, HashTreePSet.singleton(cid))))
            } else {
              finishedWork.set(true)
              Flowable.empty()
            }
          }
        }
      }

    val counter = new AtomicInteger(0)
    Bot.wire(appId, ledgerClient, transactionFilter, bot, c => counter)

    // when the bot is wired-up, no command should have been submitted to the server
    ledgerClient.submitted.size shouldBe 0
    counter.get shouldBe 0

    // when the bot receives a transaction, a command should be submitted to the server
    val createdEvent1 = create(party, templateId, id = 1)
    transactions.emit(transactionArray(createdEvent1))

    while (!finishedWork.get) Thread.sleep(1)

    ledgerClient.submitted.size shouldBe 3
    counter.get shouldBe 3
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
        new LedgerOffset.Absolute("0"))

    val parties = new java.util.HashMap[String, Filter]()
    parties.put(party, new InclusiveFilter(Set(templateId).asJava).asInstanceOf[Filter])
    val transactionFilter = new FiltersByParty(parties)

    // the "mirror" bot is a bot that for each contract found emits one exercise command.
    // In this way we can see how many commands are submitted to the ledger-client and
    // verify the result
    val atomicCount = new AtomicInteger(0)
    val bot =
      new java.util.function.Function[LedgerView[CreatedContract], Flowable[CommandsAndPendingSet]] {

        private val logger = LoggerFactory.getLogger("TestBot")

        override def apply(fcs: LedgerView[CreatedContract]): Flowable[CommandsAndPendingSet] = {
          logger.debug(s"apply(fcs: $fcs)")
          Flowable.fromIterable(
            fcs
              .getContracts(templateId)
              .asScala
              .map {
                case (contractId, _) =>
                  val command =
                    new ExerciseCommand(templateId, contractId, "choice", DAMLUnit.getInstance())
                  logger.debug(s"exercise: $command")
                  val commandList = List[Command](command).asJava
                  val commands =
                    new SubmitCommandsRequest(
                      "wid",
                      appId,
                      s"commandId_${atomicCount.incrementAndGet()}",
                      party,
                      zeroTimestamp,
                      zeroTimestamp,
                      commandList
                    )
                  logger.debug(s"commands: $commands")
                  new CommandsAndPendingSet(
                    commands,
                    HashTreePMap.singleton(templateId, HashTreePSet.singleton(contractId)))
              }
              .asJava
          )
        }
      }
    Bot.wireSimple(appId, ledgerClient, transactionFilter, bot)

    // when the bot is wired-up, no command should have been submitted to the server
    Thread.sleep(1l)
    ledgerClient.submitted.size shouldBe 0

    // when the bot receives a transaction, a command should be submitted to the server
    val createdEvent1 = create(party, templateId)
    transactions.emit(transactionArray(createdEvent1))
    Thread.sleep(1l)
    ledgerClient.submitted.size shouldBe 1

    val archivedEvent1 = archive(createdEvent1)
    val createEvent2 = create(party, templateId)
    val createEvent3 = create(party, templateId)
    transactions.emit(transactionArray(archivedEvent1, createEvent2, createEvent3))
    Thread.sleep(1l)
    ledgerClient.submitted.size shouldBe 3

    // we complete the first command with success and then check that the client hasn't submitted a new command
    commandCompletions.emit(
      new CompletionStreamResponse(
        Optional.of(new Checkpoint(zeroTimestamp, new LedgerOffset.Absolute(""))),
        List(
          scalaAPI.CompletionOuterClass.Completion
            .newBuilder()
            .setCommandId("commandId_0")
            .setStatus(Status.newBuilder().setCode(Code.OK.value()).build())
            .build()).asJava
      ))
    Thread.sleep(1l)
    ledgerClient.submitted.size shouldBe 3

    // WARNING: THE FOLLOWING TEST IS NOT PASSING YET
//    // we complete the second command with failure and then check that the client has submitted a new command
//    commandCompletionsEmitter.emit(new CompletionStreamResponse(new Checkpoint(zeroTimestamp, new LedgerOffset.Absolute("")),
//      List(CompletionOuterClass.Completion.newBuilder().setCommandId("commandId_1").setStatus(Status.newBuilder().setCode(Code.INVALID_ARGUMENT.value())).build()).asJava))
//    Thread.sleep(1l)
//    ledgerClient.submitted.size shouldBe 4
  }

  it should "query first the ACS and then the LedgerEnd sequentially so that there is not race condition and LedgerEnd >= ACS offset" in {

    /**
      * This tests that the Bot has finished dealing with the ACS before asking for the LedgerEnd.
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
      * Note that the tests delays sending the ACS to the Bot in order to "guarantee" that the test is not green because
      * or race conditions.
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
      TransactionServiceImpl.LedgerItem(
        "",
        "",
        "",
        com.google.protobuf.timestamp.Timestamp.defaultInstance,
        Seq(),
        offset,
        None)
    val getTransactionsResponses = Observable.defer[TransactionServiceImpl.LedgerItem](
      () =>
        Observable.fromArray(
          dummyTransactionResponse(if (getACSDone.future.isCompleted) rightOffset else wrongOffset))
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
      Future.successful(scalaAPI.package_service.GetPackageStatusResponse.defaultInstance)
    ) { (server, _) =>
      val client =
        DamlLedgerClient.forHostWithLedgerIdDiscovery("localhost", server.getPort, Optional.empty())
      client.connect()

      /* The bot is wired here and inside wire is where the race condition can happen. We catch the possible
       * error to pretty-print it. This try-catch depends on the implementation of `TransactionServiceImpl.getTransactionTree()`
       */
      try {
        Bot.wireSimple(
          "appId",
          client,
          new FiltersByParty(Collections.emptyMap()),
          _ => Flowable.empty())
      } catch {
        case e: io.grpc.StatusRuntimeException
            if e.getStatus.getCode.equals(Code.INVALID_ARGUMENT) =>
          /** the tests relies on specific implementation of the [[TransactionServiceImpl.getTransactions()]]  */
          fail(e.getTrailers.get(Metadata.Key.of("cause", Metadata.ASCII_STRING_MARSHALLER)))
      }

      // test is passed, we wait a bit to avoid issues with gRPC and then close the client. If there is an exception,
      // we just ignore it as there are some problems with gRPC
      try {
        Thread.sleep(100l)
        client.close()
      } catch {
        case NonFatal(e) =>
          logger.warn(
            s"Closing DamlLedgerClient caused an error, ignoring it because it can happen and it should not be a problem. Error is $e")
      }
    }
  }

  def create(party: String, templateId: Identifier, id: Int = random.nextInt()): CreatedEvent =
    new CreatedEvent(
      List(party).asJava,
      s"eid_$id",
      templateId,
      s"cid_$id",
      new Record(List.empty[Record.Field].asJava),
      Optional.empty(),
      Optional.empty(),
      Collections.emptySet(),
      Collections.emptySet()
    )

  def archive(event: CreatedEvent): ArchivedEvent =
    new ArchivedEvent(
      event.getWitnessParties,
      s"${event.getEventId}_archive",
      event.getTemplateId,
      event.getContractId)

  def transaction(events: List[Event]): Transaction =
    new Transaction(
      "tid",
      s"cid_${random.nextInt()}",
      "wid",
      zeroTimestamp,
      events.asJava,
      events.size.toString)

  def transactionWithOffset(offset: String, events: List[Event]): Transaction =
    new Transaction("tid", s"cid_${random.nextInt()}", "wid", zeroTimestamp, events.asJava, offset)

  def transactionArray(events: Event*): Transaction = transaction(events.toList)
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TestFlowable[A](name: String) extends Flowable[A] {

  private val buffer: mutable.Buffer[A] = mutable.Buffer.empty[A]
  private var isComplete: Boolean = false

  private val logger = LoggerFactory.getLogger(s"TestFlowable($name)")

  private var observerMay = Option.empty[Subscriber[_ >: A]]

  override def subscribeActual(observer: Subscriber[_ >: A]): Unit = {
    this.observerMay match {
      case Some(oldObserver) =>
        throw new RuntimeException(
          this.getClass.getSimpleName + " supports only one subscriber. Currently" +
            " subscribed " + oldObserver.toString + ", want to subscribe " + observer.toString)
      case None =>
        this.observerMay = Option(observer)
        logger.debug("Subscribed observer " + observer.toString)
        observer.onSubscribe(new Subscription {
          override def request(n: Long): Unit = ()

          override def cancel(): Unit = ()
        })
        buffer.foreach(a => observer.onNext(a))
        buffer.clear()
        if (this.isComplete) {
          observer.onComplete()
        }
    }
  }

  def emit(a: A): Unit = {
    this.observerMay match {
      case None =>
        logger.debug(s"no observer, buffering $a")
        this.buffer.append(a)
      case Some(observer) =>
        logger.debug(s"emitting $a to subscribed $observer")
        observer.onNext(a)
    }
  }

  def complete(): Unit = {
    this.observerMay match {
      case None =>
        logger.debug("no observer, buffering onComplete")
      case Some(observer) =>
        logger.debug(s"calling onComplete() on subscribed $observer")
        observer.onComplete()
    }
  }
}

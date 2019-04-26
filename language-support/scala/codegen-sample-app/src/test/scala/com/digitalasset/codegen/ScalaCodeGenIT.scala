// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.codegen.util.TestUtil.{TestContext, findOpenPort, requiredResource}
import com.digitalasset.grpc.adapter.AkkaExecutionSequencerPool
import com.digitalasset.ledger.api.refinements.ApiTypes.{CommandId, WorkflowId}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.api.v1.event.Event
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.package_service.ListPackagesResponse
import com.digitalasset.ledger.api.v1.trace_context.TraceContext
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.binding.DomainTransactionMapper.DecoderType
import com.digitalasset.ledger.client.binding.{Contract, Template, Primitive => P}
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.platform.sandbox.SandboxApplication
import com.digitalasset.platform.sandbox.SandboxApplication.SandboxServer
import com.digitalasset.platform.sandbox.config.{DamlPackageContainer, LedgerIdMode, SandboxConfig}
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.sample.EventDecoder
import com.digitalasset.sample.MyMain.NameClashRecordVariant.NameClashRecordVariantA
import com.digitalasset.sample.MyMain.{
  CallablePayout,
  Maybes,
  TextMapInt,
  OptTextMapInt,
  TextMapTextMapInt,
  ListTextMapInt,
  MkListExample,
  MyRecord,
  MyVariant,
  NameClashRecord,
  NameClashVariant,
  PayOut,
  RecordWithNestedMyVariant,
  TemplateWith23Arguments,
  TemplateWithCustomTypes,
  TemplateWithNestedRecordsAndVariants,
  TemplateWithSelfReference,
  TemplateWithUnitParam,
  VariantWithRecordWithVariant
}
import com.digitalasset.sample.MySecondMain
import com.digitalasset.util.Ctx
import com.google.protobuf.empty.Empty
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ScalaCodeGenIT
    extends AsyncWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Inside {

  private val shortTimeout = 5.seconds

  private implicit val ec: ExecutionContext = ExecutionContext.global

  implicit override lazy val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(250, Millis))

  private val ledgerId = this.getClass.getSimpleName

  private val archives = List(
    requiredResource("language-support/scala/codegen-sample-app/MyMain.dar"),
    requiredResource("language-support/scala/codegen-sample-app/MySecondMain.dar"),
  )

  private val asys = ActorSystem()
  private val amat = ActorMaterializer()(asys)
  private val aesf = new AkkaExecutionSequencerPool("clientPool")(asys)

  private val port: Int = findOpenPort().fold(e => throw new IllegalStateException(e), identity)

  private val serverConfig = SandboxConfig.default.copy(
    port = port,
    damlPackageContainer = DamlPackageContainer(archives),
    timeProviderType = TimeProviderType.WallClock,
    ledgerIdMode = LedgerIdMode.Predefined(ledgerId),
  )

  private val sandbox: SandboxServer = SandboxApplication(serverConfig)
  sandbox.start()

  private val applicationId = ledgerId + "-client"
  private val decoder: DecoderType = EventDecoder.createdEventToContractRef
  private val timeProvider = TimeProvider.UTC
  private val traceContext = TraceContext(1L, 2L, 3L, Some(4L))

  private val alice = P.Party("Alice")
  private val bob = P.Party("Bob")
  private val charlie = P.Party("Charlie")

  private val emptyCommandId = CommandId("")

  override protected def afterAll(): Unit = {
    sandbox.close()
    aesf.close()
    amat.shutdown()
    asys.terminate()
    super.afterAll()
  }

  private val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId,
    ledgerIdRequirement = LedgerIdRequirement(ledgerId, enabled = true),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )

  private val ledgerF: Future[LedgerClient] =
    LedgerClient.singleHost("127.0.0.1", port, clientConfig)(ec, aesf)

  private val ledger: LedgerClient = Await.result(ledgerF, shortTimeout)

  "generated package ID among those returned by the packageClient" in {
    val expectedPackageId: String = P.TemplateId
      .unapply(CallablePayout.id)
      .map(_._1)
      .getOrElse(fail("Cannot retrieve a package ID from the generated CallablePayout.id"))

    whenReady(ledger.packageClient.listPackages()) { response: ListPackagesResponse =>
      response.packageIds should contain(expectedPackageId)
    }
  }

  "alice creates CallablePayout contract and receives corresponding event" in {
    val contract = CallablePayout(giver = alice, receiver = bob)
    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates MkListExample contract and receives corresponding event" in {
    val contract = MkListExample(alice, P.List(1, 2, 3))
    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithSelfReference contract and receives corresponding event" in {
    import com.digitalasset.sample.MyMain
    val parent = MyMain.Maybe.Nothing(())
    val contract = TemplateWithSelfReference(alice, parent)
    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithCustomTypes contract with ProductArity variant and receives corresponding event from the ledger" in {
    val nameClashRecord =
      NameClashRecord(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)
    val nameClashVariant = NameClashVariant.ProductArity("test")
    val nameClashRecordVariant = NameClashRecordVariantA(1, 2, 3)
    val contract =
      TemplateWithCustomTypes(alice, nameClashRecord, nameClashVariant, nameClashRecordVariant)

    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithCustomTypes contract with a NotifyAll variant and receives corresponding event from the ledger" in {
    val nameClashRecord =
      NameClashRecord(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)
    val nameClashVariant = NameClashVariant.NotifyAll(100L)
    val nameClashRecordVariant = NameClashRecordVariantA(1, 2, 3)
    val contract =
      TemplateWithCustomTypes(alice, nameClashRecord, nameClashVariant, nameClashRecordVariant)

    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithUnitParam contract and receives corresponding event from the ledger" in {
    val contract = TemplateWithUnitParam(alice)
    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithNestedRecordsAndVariants contract and receives corresponding event from the ledger" in {
    val boolVal = true
    val time: P.Timestamp =
      P.Timestamp.discardNanos(Instant.now).getOrElse(fail("Can't create time instance"))
    val myRecord =
      MyRecord(1, BigDecimal("1.2"), alice, "Text", time, (), boolVal, List(10, 20, 30))
    val myVariant = MyVariant.MyVariantA(())
    val recordWithNestedMyVariant =
      RecordWithNestedMyVariant(MyVariant.MyVariantA(()), MyVariant.MyVariantB(()))
    val variantWithRecordWithVariant =
      VariantWithRecordWithVariant.VariantWithRecordWithVariantA(recordWithNestedMyVariant)
    val contract = TemplateWithNestedRecordsAndVariants(
      alice,
      myRecord,
      myVariant,
      recordWithNestedMyVariant,
      variantWithRecordWithVariant)
    testCreateContractAndReceiveEvent(contract, alice)
  }

  private def assertCommandStatus[A](ctx: Ctx[A, Try[Empty]])(expectedContext: A): Assertion = {
    ctx.context shouldBe expectedContext
    ctx.value match {
      case Success(_) =>
        succeed
      case Failure(e) =>
        fail(e)
    }
  }

  "alice creates CallablePayout contract, bob exercises Call choice" in {

    val contextId = TestContext("create_CallablePayout_exercise_Call-" + uniqueId)
    val createCommandId = CommandId(uniqueId)
    val exerciseCommandId = CommandId(uniqueId)
    val createWorkflowId = WorkflowId(uniqueId)
    val exerciseWorkflowId = WorkflowId(uniqueId)

    val exerciseTxF = for {
      offset0 <- ledgerEnd()
      _ <- aliceCreateCallablePayout(contextId, createWorkflowId, createCommandId)
      aliceTx0 <- nextTransaction(alice)(offset0)
      _ <- Future(assertTransaction(aliceTx0)(createCommandId, createWorkflowId))
      bobTx0 <- nextTransaction(bob)(offset0)
      _ <- Future(assertTransaction(bobTx0)(emptyCommandId, createWorkflowId))
      offset1 <- ledgerEnd()
      _ <- bobExerciseCall(bobTx0)(contextId, exerciseWorkflowId, exerciseCommandId)
      bobTx1 <- nextTransaction(bob)(offset1)
      _ <- Future(assertTransaction(bobTx1)(exerciseCommandId, exerciseWorkflowId))
      aliceTx1 <- nextTransaction(alice)(offset1)
      _ <- Future(assertTransaction(aliceTx1)(emptyCommandId, exerciseWorkflowId))
    } yield bobTx1

    whenReady(exerciseTxF) { tx =>
      tx.workflowId shouldBe exerciseWorkflowId
      inside(tx.events) {
        case Seq(archiveEvent, createEvent) =>
          archiveEvent.event.isArchived shouldBe true
          assertCreateEvent(createEvent)(PayOut(bob, alice))
      }
    }
  }

  "alice creates CallablePayout contract, bob exercises Transfer to charlie" in {

    val contextId = TestContext("create_CallablePayout_exercise_Call-" + uniqueId)
    val createCommandId = CommandId(uniqueId)
    val exerciseCommandId = CommandId(uniqueId)
    val createWorkflowId = WorkflowId(uniqueId)
    val exerciseWorkflowId = WorkflowId(uniqueId)

    val exerciseTxF: Future[(Transaction, Transaction)] = for {
      offset0 <- ledgerEnd()
      _ <- aliceCreateCallablePayout(contextId, createWorkflowId, createCommandId)
      aliceTx0 <- nextTransaction(alice)(offset0)
      _ <- Future(assertTransaction(aliceTx0)(createCommandId, createWorkflowId))
      bobTx0 <- nextTransaction(bob)(offset0)
      _ <- Future(assertTransaction(bobTx0)(emptyCommandId, createWorkflowId))
      offset1 <- ledgerEnd()
      _ <- bobExerciseTransfer(bobTx0, charlie)(contextId, exerciseWorkflowId, exerciseCommandId)
      bobTx1 <- nextTransaction(bob)(offset1)
      _ <- Future(assertTransaction(bobTx1)(exerciseCommandId, exerciseWorkflowId))
      aliceTx1 <- nextTransaction(alice)(offset1)
      _ <- Future(assertTransaction(aliceTx1)(emptyCommandId, exerciseWorkflowId))
      charlieTx1 <- nextTransaction(charlie)(offset1)
      _ <- Future(assertTransaction(aliceTx1)(emptyCommandId, exerciseWorkflowId))
    } yield (bobTx1, charlieTx1)

    whenReady(exerciseTxF) {
      case (bobTx, charlieTx) =>
        bobTx.workflowId shouldBe exerciseWorkflowId
        inside(bobTx.events) {
          case Seq(archiveEvent) =>
            archiveEvent.event.isArchived shouldBe true
        }
        charlieTx.workflowId shouldBe exerciseWorkflowId
        inside(charlieTx.events) {
          case Seq(createEvent) =>
            createEvent.event.isCreated shouldBe true
            assertCreateEvent(createEvent)(CallablePayout(alice, charlie))
        }
    }
  }

  "alice creates TemplateWith23Arguments contract and receives corresponding event" in {
    val contract = TemplateWith23Arguments(
      alice,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true)
    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates Maybes contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[Maybes].sample getOrElse sys.error("random Maybes failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates TextMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[TextMapInt].sample getOrElse sys.error("random TexMap failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates OptTextMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[OptTextMapInt].sample getOrElse sys.error(
      "random OptTextMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates TextMapTextMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[TextMapTextMapInt].sample getOrElse sys.error(
      "random TextMapTextMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates ListTextMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[ListTextMapInt].sample getOrElse sys.error(
      "random TListTextMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates DummyTemplateFromAnotherDar contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MySecondMain.DummyTemplateFromAnotherDar].sample getOrElse sys.error(
      "random DummyTemplateFromAnotherDar failed")
    testCreateContractAndReceiveEvent(contract copy (owner = alice), alice)
  }

  private def testCreateContractAndReceiveEvent(
      contract: Template[AnyRef],
      party: P.Party): Assertion = {
    val contextId = TestContext(uniqueId)
    val commandId = CommandId(uniqueId)
    val workflowId = WorkflowId(uniqueId)

    val createCommand: P.Update[P.ContractId[AnyRef]] = contract.create
    val request: SubmitRequest = submitRequest(workflowId, commandId, party, createCommand)

    val future = for {
      offset <- ledgerEnd()
      statuses <- send(contextId, request)(1)
      transaction <- nextTransaction(party)(offset)
    } yield (statuses, transaction)

    whenReady(future) {
      case (statuses, transaction) =>
        inside(statuses) {
          case Seq(ctx) =>
            assertCommandStatus(ctx)(contextId)
        }
        assertTransaction(transaction)(commandId, workflowId)
        inside(transaction.events) {
          case Seq(createEvent) =>
            assertCreateEvent(createEvent)(contract)
        }
    }
  }

  private def nextTransaction(party: P.Party)(offset: LedgerOffset): Future[Transaction] =
    ledger.transactionClient
      .getTransactions(offset, None, transactionFilter(party))
      .take(1)
      .runWith(Sink.head)(amat)

  private def uniqueId = UUID.randomUUID.toString

  private def submitRequest(
      workflowId: WorkflowId,
      commandId: CommandId,
      party: P.Party,
      seq: P.Update[P.ContractId[AnyRef]]*): SubmitRequest = {

    val now = timeProvider.getCurrentTime
    val commands = Commands(
      ledgerId = ledger.ledgerId,
      workflowId = WorkflowId.unwrap(workflowId),
      applicationId = applicationId,
      commandId = CommandId.unwrap(commandId),
      party = P.Party.unwrap(party),
      ledgerEffectiveTime = Some(fromInstant(now)),
      maximumRecordTime = Some(fromInstant(now.plus(java.time.Duration.ofSeconds(2)))),
      commands = seq.map(_.command)
    )
    SubmitRequest(Some(commands), Some(traceContext))
  }

  private def send[A](context: A, requests: SubmitRequest*)(
      take: Long): Future[Seq[Ctx[A, Try[Empty]]]] =
    send(requests.map(Ctx(context, _)): _*)(take)

  private def send[A](input: Ctx[A, SubmitRequest]*)(take: Long): Future[Seq[Ctx[A, Try[Empty]]]] =
    Source
      .fromIterator(() => input.iterator)
      .via(ledger.commandClient.submissionFlow)
      .take(take)
      .runWith(Sink.seq)(amat)

  private def toFuture[A](o: Option[A]): Future[A] = o match {
    case None => Future.failed(new IllegalStateException(s"empty option: $o"))
    case Some(x) => Future.successful(x)
  }

  private def ledgerEnd(): Future[LedgerOffset] =
    ledger.transactionClient.getLedgerEnd.flatMap(response => toFuture(response.offset))

  private def aliceCreateCallablePayout(
      contextId: TestContext,
      workflowId: WorkflowId,
      commandId: CommandId): Future[Seq[Ctx[TestContext, Try[Empty]]]] =
    send(
      contextId,
      submitRequest(
        workflowId,
        commandId,
        alice,
        CallablePayout(giver = alice, receiver = bob).create))(1)

  private def bobExerciseCall(transaction: Transaction)(
      contextId: TestContext,
      workflowId: WorkflowId,
      commandId: CommandId): Future[Seq[Ctx[TestContext, Try[Empty]]]] =
    for {
      event <- toFuture(transaction.events.headOption)
      created <- toFuture(event.event.created)
      contractId = P.ContractId[CallablePayout](created.contractId)
      exerciseCommand = contractId.exerciseCall2(bob)
      status <- send(contextId, submitRequest(workflowId, commandId, bob, exerciseCommand))(1)
    } yield status

  private def bobExerciseTransfer(transaction: Transaction, newReceiver: P.Party)(
      contextId: TestContext,
      workflowId: WorkflowId,
      commandId: CommandId): Future[Seq[Ctx[TestContext, Try[Empty]]]] =
    for {
      event <- toFuture(transaction.events.headOption)
      created <- toFuture(event.event.created)
      contractId = P.ContractId[CallablePayout](created.contractId)
      exerciseCommand = contractId.exerciseTransfer(actor = bob, newReceiver = newReceiver)
      status <- send(contextId, submitRequest(workflowId, commandId, bob, exerciseCommand))(1)
    } yield status

  private def assertTransaction(
      tx: Transaction)(expectedCommandId: CommandId, expectedWorkflowId: WorkflowId): Assertion = {
    tx.commandId shouldBe expectedCommandId
    tx.workflowId shouldBe expectedWorkflowId
  }

  private def assertCreateEvent(event: Event)(expectedContract: Template[AnyRef]): Assertion = {
    event.event.isCreated shouldBe true
    decoder(event.getCreated) match {
      case Left(e) => fail(e.toString)
      case Right(Contract(_, contract)) => contract shouldBe expectedContract
    }
  }

  private def transactionFilter(ps: P.Party*) =
    TransactionFilter(P.Party.unsubst(ps).map((_, Filters.defaultInstance)).toMap)
}

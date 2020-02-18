// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

import java.io.File
import java.time.Instant
import java.util.UUID

import akka.stream.scaladsl.{Sink, Source}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.codegen.util.TestUtil.{TestContext, requiredResource}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.{CommandId, WorkflowId}
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
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
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.SandboxFixture
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.sample.MyMain.{CallablePayout, MkListExample, PayOut}
import com.digitalasset.sample.{EventDecoder, MyMain, MySecondMain}
import com.digitalasset.util.Ctx
import com.google.protobuf.empty.Empty
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ScalaCodeGenIT
    extends AsyncWordSpec
    with Matchers
    with ScalaFutures
    with Inside
    with SuiteResourceManagementAroundAll
    with SandboxFixture {

  private val StartupTimeout = 10.seconds

  override implicit lazy val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(250, Millis))

  override implicit def executionContext: ExecutionContext = ExecutionContext.global

  override protected def packageFiles: List[File] = List(
    requiredResource("language-support/scala/codegen-sample-app/MyMain.dar"),
    requiredResource("language-support/scala/codegen-sample-app/MySecondMain.dar"),
  )

  private val ledgerId = this.getClass.getSimpleName
  private val applicationId = ledgerId + "-client"
  private val decoder: DecoderType = EventDecoder.createdEventToContractRef
  private val timeProvider = TimeProvider.UTC
  private val traceContext = TraceContext(1L, 2L, 3L, Some(4L))

  private val alice = P.Party("Alice")
  private val bob = P.Party("Bob")
  private val charlie = P.Party("Charlie")

  private val emptyCommandId = CommandId("")

  private val emptyAgreementText = Some("") // this is by design, starting from release: 0.12.18 it is a requried field

  override protected def config: SandboxConfig = super.config.copy(
    ledgerIdMode = LedgerIdMode.Static(LedgerId(ledgerId)),
    timeProviderType = Some(TimeProviderType.WallClock),
  )

  private val clientConfig = LedgerClientConfiguration(
    applicationId = applicationId,
    ledgerIdRequirement = LedgerIdRequirement(ledgerId, enabled = true),
    commandClient = CommandClientConfiguration.default,
    sslContext = None
  )

  private var ledger: LedgerClient = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    ledger = Await.result(LedgerClient(channel, clientConfig), StartupTimeout)
  }

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
    testCreateContractAndReceiveEvent(
      contract,
      alice,
      expectedAgreementText = Some(expectedAgreementAsDefinedInDaml(contract)))
  }

  private def expectedAgreementAsDefinedInDaml(contract: MkListExample): String = {
    val sum: P.Int64 = contract.xs.sum
    s"I am worth $sum"
  }

  "alice creates TemplateWithSelfReference contract and receives corresponding event" in {
    import com.digitalasset.sample.MyMain
    val parent = MyMain.Maybe.Nothing(())
    val contract = MyMain.TemplateWithSelfReference(alice, parent)
    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithCustomTypes contract with ProductArity variant and receives corresponding event from the ledger" in {
    val nameClashRecord =
      MyMain.NameClashRecord(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)
    val nameClashVariant = MyMain.NameClashVariant.ProductArity("test")
    val nameClashRecordVariant = MyMain.NameClashRecordVariant.NameClashRecordVariantA(1, 2, 3)
    val contract =
      MyMain.TemplateWithCustomTypes(
        alice,
        nameClashRecord,
        nameClashVariant,
        nameClashRecordVariant)

    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithCustomTypes contract with a NotifyAll variant and receives corresponding event from the ledger" in {
    val nameClashRecord =
      MyMain.NameClashRecord(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)
    val nameClashVariant = MyMain.NameClashVariant.NotifyAll(100L)
    val nameClashRecordVariant = MyMain.NameClashRecordVariant.NameClashRecordVariantA(1, 2, 3)
    val contract =
      MyMain.TemplateWithCustomTypes(
        alice,
        nameClashRecord,
        nameClashVariant,
        nameClashRecordVariant)

    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithUnitParam contract and receives corresponding event from the ledger" in {
    val contract = MyMain.TemplateWithUnitParam(alice)
    testCreateContractAndReceiveEvent(contract, alice)
  }

  "alice creates TemplateWithNestedRecordsAndVariants contract and receives corresponding event from the ledger" in {
    val boolVal = true
    val time: P.Timestamp =
      P.Timestamp.discardNanos(Instant.now).getOrElse(fail("Can't create time instance"))
    val myRecord =
      MyMain.MyRecord(1, BigDecimal("1.2"), alice, "Text", time, (), boolVal, List(10, 20, 30))
    val myVariant = MyMain.MyVariant.MyVariantA(())
    val myEnum = MyMain.MyEnum.MyEnumA
    val recordWithNestedMyVariant =
      MyMain.RecordWithNestedMyVariantMyEnum(MyMain.MyVariant.MyVariantB(()), MyMain.MyEnum.MyEnumB)
    val variantWithRecordWithVariant =
      MyMain.VariantWithRecordWithVariant.VariantWithRecordWithVariantA(recordWithNestedMyVariant)
    val contract = MyMain.TemplateWithNestedRecordsVariantsAndEnums(
      alice,
      myRecord,
      myVariant,
      myEnum,
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
          val payOut = PayOut(receiver = bob, giver = alice)
          assertCreateEvent(createEvent)(payOut, Some(expectedAgreementAsDefinedInDaml(payOut)))
      }
    }
  }

  private def expectedAgreementAsDefinedInDaml(contract: PayOut): String =
    s"'${P.Party.unwrap(contract.giver): String}' must pay to '${P.Party.unwrap(contract.receiver): String}' the sum of five pounds."

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
            assertCreateEvent(createEvent)(CallablePayout(alice, charlie), emptyAgreementText)
        }
    }
  }

  "alice creates TemplateWith23Arguments contract and receives corresponding event" in {
    //noinspection NameBooleanParameters
    val contract = MyMain.TemplateWith23Arguments(
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
    val contract = arbitrary[MyMain.Maybes].sample getOrElse sys.error("random Maybes failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates TextMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.TextMapInt].sample getOrElse sys.error("random TexMap failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates OptTextMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.OptTextMapInt].sample getOrElse sys.error(
      "random OptTextMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates TextMapTextMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.TextMapTextMapInt].sample getOrElse sys.error(
      "random TextMapTextMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates TextMapText contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.TextMapText].sample getOrElse sys.error(
      "random TextMapText failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates ListTextMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.ListTextMapInt].sample getOrElse sys.error(
      "random ListTextMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates OptMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.OptMapInt].sample getOrElse sys.error("random OptMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates ListMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.ListMapInt].sample getOrElse sys.error(
      "random ListMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates MapMapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.MapMapInt].sample getOrElse sys.error("random MapMapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates MapInt contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.MapInt].sample getOrElse sys.error("random MapInt failed")
    testCreateContractAndReceiveEvent(contract copy (party = alice), alice)
  }

  "alice creates DummyTemplateFromAnotherDar contract and receives corresponding event" in {
    import com.digitalasset.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MySecondMain.DummyTemplateFromAnotherDar].sample getOrElse sys.error(
      "random DummyTemplateFromAnotherDar failed")
    testCreateContractAndReceiveEvent(contract copy (owner = alice), alice)
  }

  "alice creates-and-exercises SimpleListExample with Go and receives corresponding event" in {
    val contract = MyMain.SimpleListExample(alice, P.List(42))
    val exerciseConsequence = MkListExample(alice, P.List(42))
    testCommandAndReceiveEvent(
      contract.createAnd.exerciseGo(alice),
      alice,
      assertCreateEvent(_)(
        exerciseConsequence,
        Some(expectedAgreementAsDefinedInDaml(exerciseConsequence))))
  }

  private def testCreateContractAndReceiveEvent(
      contract: Template[AnyRef],
      party: P.Party,
      expectedAgreementText: Option[String] = emptyAgreementText): Assertion =
    testCommandAndReceiveEvent(
      contract.create,
      party,
      assertCreateEvent(_)(contract, expectedAgreementText))

  private def testCommandAndReceiveEvent(
      command: P.Update[_],
      party: P.Party,
      checkResult: Event => Assertion): Assertion = {
    val contextId = TestContext(uniqueId)
    val commandId = CommandId(uniqueId)
    val workflowId = WorkflowId(uniqueId)

    val request: SubmitRequest = submitRequest(workflowId, commandId, party, command)

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
          case Seq(event) =>
            checkResult(event)
        }
    }
  }

  private def nextTransaction(party: P.Party)(offset: LedgerOffset): Future[Transaction] =
    ledger.transactionClient
      .getTransactions(offset, None, transactionFilter(party))
      .take(1)
      .runWith(Sink.head)

  private def uniqueId = UUID.randomUUID.toString

  private def submitRequest(
      workflowId: WorkflowId,
      commandId: CommandId,
      party: P.Party,
      seq: P.Update[_]*): SubmitRequest = {

    val now = timeProvider.getCurrentTime
    val commands = Commands(
      ledgerId = ledger.ledgerId.unwrap,
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
      .via(ledger.commandClient.submissionFlow())
      .take(take)
      .runWith(Sink.seq)

  private def toFuture[A](o: Option[A]): Future[A] = o match {
    case None => Future.failed(new IllegalStateException(s"empty option: $o"))
    case Some(x) => Future.successful(x)
  }

  private def ledgerEnd(): Future[LedgerOffset] =
    ledger.transactionClient.getLedgerEnd().flatMap(response => toFuture(response.offset))

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

  private def assertCreateEvent(event: Event)(
      expectedContract: Template[AnyRef],
      expectedAgreement: Option[String]): Assertion = {
    event.event.isCreated shouldBe true
    decoder(event.getCreated) match {
      case Left(e) => fail(e.toString)
      case Right(Contract(_, contract, agreementText, signatories, observers, key)) =>
        contract shouldBe expectedContract
        agreementText shouldBe expectedAgreement
        agreementText shouldBe event.getCreated.agreementText
        signatories shouldBe event.getCreated.signatories
        observers shouldBe event.getCreated.observers
        key shouldBe event.getCreated.contractKey
    }
  }

  private def transactionFilter(ps: P.Party*) =
    TransactionFilter(P.Party.unsubst(ps).map((_, Filters.defaultInstance)).toMap)
}

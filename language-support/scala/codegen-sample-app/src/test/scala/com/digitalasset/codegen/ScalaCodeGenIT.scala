// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import com.daml.codegen.util.TestUtil.{TestContext, requiredResource}
import com.daml.integrationtest.CantonFixture
import com.daml.ledger.api.refinements.ApiTypes.{CommandId, WorkflowId}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.binding.DomainTransactionMapper.DecoderType
import com.daml.ledger.client.binding.{Contract, Template, Primitive => P}
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.sample.MyMain.{CallablePayout, MkListExample, PayOut}
import com.daml.sample.{EventDecoder, MyMain, MySecondMain}
import com.daml.util.Ctx
import com.google.protobuf.empty.Empty
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

final case class ScalaCodeGenITFixture(
    ledger: LedgerClient,
    alice: P.Party,
    bob: P.Party,
    charlie: P.Party,
)

class ScalaCodeGenIT
    extends AsyncWordSpec
    with Matchers
    with ScalaFutures
    with Inside
    with SuiteResourceManagementAroundAll
    with CantonFixture {

  override protected lazy val darFiles: List[Path] = List(
    requiredResource("language-support/scala/codegen-sample-app/MyMain.dar"),
    requiredResource("language-support/scala/codegen-sample-app/MySecondMain.dar"),
  ).map(_.toPath)

  override implicit lazy val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(250, Millis))

  private val decoder: DecoderType = EventDecoder.createdEventToContractRef

  private val emptyCommandId = CommandId("")

  private val emptyAgreementText = Some(
    ""
  ) // this is by design, starting from release: 0.12.18 it is a required field

  private def withFixture[A](
      testFn: ScalaCodeGenITFixture => Future[A]
  ): Future[A] = for {
    ledger <- defaultLedgerClient()
    parties <- Future.sequence(
      List("alice", "bob", "charlie").map(p =>
        ledger.partyManagementClient
          .allocateParty(None, Some(p))
          .map(p => P.Party(p.party.toString))
      )
    )
    a <- testFn(ScalaCodeGenITFixture(ledger, parties(0), parties(1), parties(2)))
  } yield a

  "generated package ID among those returned by the packageClient" in withFixture { fixture =>
    import fixture.ledger
    val expectedPackageId: String = P.TemplateId
      .unapply(CallablePayout.id)
      .map(_._1)
      .getOrElse(fail("Cannot retrieve a package ID from the generated CallablePayout.id"))

    for {
      response <- ledger.packageClient.listPackages()
    } yield response.packageIds should contain(expectedPackageId)
  }

  "alice creates CallablePayout contract and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice, bob}
      val contract = CallablePayout(giver = alice, receiver = bob)
      testCreateContractAndReceiveEvent(ledger, contract, alice)
  }

  "alice creates MkListExample contract and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      val contract = MkListExample(alice, P.List(1, 2, 3))
      testCreateContractAndReceiveEvent(
        ledger,
        contract,
        alice,
        expectedAgreementText = Some(expectedAgreementAsDefinedInDaml(contract)),
      )
  }

  private def expectedAgreementAsDefinedInDaml(contract: MkListExample): String = {
    val sum: P.Int64 = contract.xs.sum
    s"I am worth $sum"
  }

  "alice creates TemplateWithSelfReference contract and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      import com.daml.sample.MyMain
      val parent = MyMain.Maybe.Nothing(())
      val contract = MyMain.TemplateWithSelfReference(alice, parent)
      testCreateContractAndReceiveEvent(ledger, contract, alice)
  }

  "alice creates TemplateWithCustomTypes contract with ProductArity variant and receives corresponding event from the ledger" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      val nameClashRecord =
        MyMain.NameClashRecord(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)
      val nameClashVariant = MyMain.NameClashVariant.ProductArity("test")
      val nameClashRecordVariant = MyMain.NameClashRecordVariant.NameClashRecordVariantA(1, 2, 3)
      val contract =
        MyMain.TemplateWithCustomTypes(
          alice,
          nameClashRecord,
          nameClashVariant,
          nameClashRecordVariant,
        )

      testCreateContractAndReceiveEvent(ledger, contract, alice)
  }

  "alice creates TemplateWithCustomTypes contract with a NotifyAll variant and receives corresponding event from the ledger" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      val nameClashRecord =
        MyMain.NameClashRecord(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)
      val nameClashVariant = MyMain.NameClashVariant.NotifyAll(100L)
      val nameClashRecordVariant = MyMain.NameClashRecordVariant.NameClashRecordVariantA(1, 2, 3)
      val contract =
        MyMain.TemplateWithCustomTypes(
          alice,
          nameClashRecord,
          nameClashVariant,
          nameClashRecordVariant,
        )

      testCreateContractAndReceiveEvent(ledger, contract, alice)
  }

  "alice creates TemplateWithUnitParam contract and receives corresponding event from the ledger" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      val contract = MyMain.TemplateWithUnitParam(alice)
      testCreateContractAndReceiveEvent(ledger, contract, alice)
  }

  "alice creates TemplateWithNestedRecordsAndVariants contract and receives corresponding event from the ledger" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      val boolVal = true
      val time: P.Timestamp =
        P.Timestamp.discardNanos(Instant.now).getOrElse(fail("Can't create time instance"))
      val myRecord =
        MyMain.MyRecord(1, BigDecimal("1.2"), alice, "Text", time, (), boolVal, List(10, 20, 30))
      val myVariant = MyMain.MyVariant.MyVariantA(())
      val myEnum = MyMain.MyEnum.MyEnumA
      val recordWithNestedMyVariant =
        MyMain.RecordWithNestedMyVariantMyEnum(
          MyMain.MyVariant.MyVariantB(()),
          MyMain.MyEnum.MyEnumB,
        )
      val variantWithRecordWithVariant =
        MyMain.VariantWithRecordWithVariant.VariantWithRecordWithVariantA(recordWithNestedMyVariant)
      val contract = MyMain.TemplateWithNestedRecordsVariantsAndEnums(
        alice,
        myRecord,
        myVariant,
        myEnum,
        recordWithNestedMyVariant,
        variantWithRecordWithVariant,
      )
      testCreateContractAndReceiveEvent(ledger, contract, alice)
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

  "alice creates CallablePayout contract, bob exercises Call choice" in withFixture { fixture =>
    import fixture.{ledger, alice, bob}

    val contextId = TestContext("create_CallablePayout_exercise_Call-" + uniqueId)
    val createCommandId = CommandId(uniqueId)
    val exerciseCommandId = CommandId(uniqueId)
    val createWorkflowId = WorkflowId(uniqueId)
    val exerciseWorkflowId = WorkflowId(uniqueId)

    for {
      offset0 <- ledgerEnd(ledger)
      _ <- createCallablePayout(ledger, alice, bob, contextId, createWorkflowId, createCommandId)
      aliceTx0 <- nextTransaction(ledger, alice)(offset0)
      _ <- Future(assertTransaction(aliceTx0)(createCommandId, createWorkflowId))
      bobTx0 <- nextTransaction(ledger, bob)(offset0)
      _ <- Future(assertTransaction(bobTx0)(emptyCommandId, createWorkflowId))
      offset1 <- ledgerEnd(ledger)
      _ <- exerciseCall(ledger, bob, bobTx0)(contextId, exerciseWorkflowId, exerciseCommandId)
      bobTx1 <- nextTransaction(ledger, bob)(offset1)
      _ <- Future(assertTransaction(bobTx1)(exerciseCommandId, exerciseWorkflowId))
      aliceTx1 <- nextTransaction(ledger, alice)(offset1)
      _ <- Future(assertTransaction(aliceTx1)(emptyCommandId, exerciseWorkflowId))
    } yield {
      bobTx1.workflowId shouldBe exerciseWorkflowId
      inside(bobTx1.events) { case Seq(archiveEvent, createEvent) =>
        archiveEvent.event.isArchived shouldBe true
        val payOut = PayOut(receiver = bob, giver = alice)
        assertCreateEvent(createEvent)(payOut, Some(expectedAgreementAsDefinedInDaml(payOut)))
      }
    }
  }

  private def expectedAgreementAsDefinedInDaml(contract: PayOut): String =
    s"'${P.Party.unwrap(contract.giver): String}' must pay to '${P.Party.unwrap(contract.receiver): String}' the sum of five pounds."

  "alice creates CallablePayout contract, bob exercises Transfer to charlie" in withFixture {
    fixture =>
      import fixture.{ledger, alice, bob, charlie}

      val contextId = TestContext("create_CallablePayout_exercise_Call-" + uniqueId)
      val createCommandId = CommandId(uniqueId)
      val exerciseCommandId = CommandId(uniqueId)
      val createWorkflowId = WorkflowId(uniqueId)
      val exerciseWorkflowId = WorkflowId(uniqueId)

      for {
        offset0 <- ledgerEnd(ledger)
        _ <- createCallablePayout(ledger, alice, bob, contextId, createWorkflowId, createCommandId)
        aliceTx0 <- nextTransaction(ledger, alice)(offset0)
        _ <- Future(assertTransaction(aliceTx0)(createCommandId, createWorkflowId))
        bobTx0 <- nextTransaction(ledger, bob)(offset0)
        _ <- Future(assertTransaction(bobTx0)(emptyCommandId, createWorkflowId))
        offset1 <- ledgerEnd(ledger)
        _ <- exerciseTransfer(ledger, bob, charlie, bobTx0)(
          contextId,
          exerciseWorkflowId,
          exerciseCommandId,
        )
        bobTx1 <- nextTransaction(ledger, bob)(offset1)
        _ <- Future(assertTransaction(bobTx1)(exerciseCommandId, exerciseWorkflowId))
        aliceTx1 <- nextTransaction(ledger, alice)(offset1)
        _ <- Future(assertTransaction(aliceTx1)(emptyCommandId, exerciseWorkflowId))
        charlieTx1 <- nextTransaction(ledger, charlie)(offset1)
        _ <- Future(assertTransaction(aliceTx1)(emptyCommandId, exerciseWorkflowId))
      } yield {
        bobTx1.workflowId shouldBe exerciseWorkflowId
        inside(bobTx1.events) { case Seq(archiveEvent) =>
          archiveEvent.event.isArchived shouldBe true
        }
        charlieTx1.workflowId shouldBe exerciseWorkflowId
        inside(charlieTx1.events) { case Seq(createEvent) =>
          createEvent.event.isCreated shouldBe true
          assertCreateEvent(createEvent)(CallablePayout(alice, charlie), emptyAgreementText)
        }
      }
  }

  "alice creates TemplateWith23Arguments contract and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      // noinspection NameBooleanParameters
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
        true,
      )
      testCreateContractAndReceiveEvent(ledger, contract, alice)
  }

  "alice creates Maybes contract and receives corresponding event" in withFixture { fixture =>
    import fixture.{ledger, alice}
    import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.Maybes].sample getOrElse sys.error("random Maybes failed")
    testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates TextMapInt contract and receives corresponding event" in withFixture { fixture =>
    import fixture.{ledger, alice}
    import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.TextMapInt].sample getOrElse sys.error("random TexMap failed")
    testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates OptTextMapInt contract and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
      val contract =
        arbitrary[MyMain.OptTextMapInt].sample getOrElse sys.error("random OptTextMapInt failed")
      testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates TextMapTextMapInt contract and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
      val contract = arbitrary[MyMain.TextMapTextMapInt].sample getOrElse sys.error(
        "random TextMapTextMapInt failed"
      )
      testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates TextMapText contract and receives corresponding event" in withFixture { fixture =>
    import fixture.{ledger, alice}
    import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract =
      arbitrary[MyMain.TextMapText].sample getOrElse sys.error("random TextMapText failed")
    testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates ListTextMapInt contract and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
      val contract =
        arbitrary[MyMain.ListTextMapInt].sample getOrElse sys.error("random ListTextMapInt failed")
      testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates OptMapInt contract and receives corresponding event" in withFixture { fixture =>
    import fixture.{ledger, alice}
    import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.OptMapInt].sample getOrElse sys.error("random OptMapInt failed")
    testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates ListMapInt contract and receives corresponding event" in withFixture { fixture =>
    import fixture.{ledger, alice}
    import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract =
      arbitrary[MyMain.ListMapInt].sample getOrElse sys.error("random ListMapInt failed")
    testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates MapMapInt contract and receives corresponding event" in withFixture { fixture =>
    import fixture.{ledger, alice}
    import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.MapMapInt].sample getOrElse sys.error("random MapMapInt failed")
    testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates MapInt contract and receives corresponding event" in withFixture { fixture =>
    import fixture.{ledger, alice}
    import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
    val contract = arbitrary[MyMain.MapInt].sample getOrElse sys.error("random MapInt failed")
    testCreateContractAndReceiveEvent(ledger, contract copy (party = alice), alice)
  }

  "alice creates DummyTemplateFromAnotherDar contract and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      import com.daml.ledger.client.binding.encoding.GenEncoding.Implicits._
      val contract = arbitrary[MySecondMain.DummyTemplateFromAnotherDar].sample getOrElse sys.error(
        "random DummyTemplateFromAnotherDar failed"
      )
      testCreateContractAndReceiveEvent(ledger, contract copy (owner = alice), alice)
  }

  "alice creates-and-exercises SimpleListExample with Go and receives corresponding event" in withFixture {
    fixture =>
      import fixture.{ledger, alice}
      val contract = MyMain.SimpleListExample(alice, P.List(42))
      val exerciseConsequence = MkListExample(alice, P.List(42))
      testCommandAndReceiveEvent(
        ledger,
        contract.createAnd.exerciseGo(),
        alice,
        assertCreateEvent(_)(
          exerciseConsequence,
          Some(expectedAgreementAsDefinedInDaml(exerciseConsequence)),
        ),
      )
  }

  private def testCreateContractAndReceiveEvent(
      ledger: LedgerClient,
      contract: Template[AnyRef],
      party: P.Party,
      expectedAgreementText: Option[String] = emptyAgreementText,
  ): Future[Assertion] =
    testCommandAndReceiveEvent(
      ledger,
      contract.create,
      party,
      assertCreateEvent(_)(contract, expectedAgreementText),
    )

  private def testCommandAndReceiveEvent(
      ledger: LedgerClient,
      command: P.Update[_],
      party: P.Party,
      checkResult: Event => Assertion,
  ): Future[Assertion] = {
    val contextId = TestContext(uniqueId)
    val commandId = CommandId(uniqueId)
    val workflowId = WorkflowId(uniqueId)

    val submission = aCommandSubmission(ledger, workflowId, commandId, party, command)

    for {
      offset <- ledgerEnd(ledger)
      statuses <- send(ledger, contextId, submission)(1)
      transaction <- nextTransaction(ledger, party)(offset)
    } yield {
      inside(statuses) { case Seq(ctx) =>
        assertCommandStatus(ctx)(contextId)
      }
      assertTransaction(transaction)(commandId, workflowId)
      inside(transaction.events) { case Seq(event) =>
        checkResult(event)
      }
    }
  }

  private def nextTransaction(ledger: LedgerClient, party: P.Party)(
      offset: LedgerOffset
  ): Future[Transaction] =
    ledger.transactionClient
      .getTransactions(offset, None, transactionFilter(party))
      .take(1)
      .runWith(Sink.head)

  private def uniqueId = UUID.randomUUID.toString

  private def aCommandSubmission(
      ledger: LedgerClient,
      workflowId: WorkflowId,
      commandId: CommandId,
      party: P.Party,
      seq: P.Update[_]*
  ): CommandSubmission =
    CommandSubmission(
      Commands(
        ledgerId = ledger.ledgerId.unwrap,
        workflowId = WorkflowId.unwrap(workflowId),
        applicationId = applicationId.getOrElse(""),
        commandId = CommandId.unwrap(commandId),
        party = P.Party.unwrap(party),
        commands = seq.map(_.command),
      )
    )

  private def send[A](ledger: LedgerClient, context: A, submissions: CommandSubmission*)(
      take: Long
  ): Future[Seq[Ctx[A, Try[Empty]]]] =
    send(ledger, submissions.map(Ctx(context, _)): _*)(take)

  private def send[A](
      ledger: LedgerClient,
      input: Ctx[A, CommandSubmission]*
  )(take: Long): Future[Seq[Ctx[A, Try[Empty]]]] =
    Source
      .fromIterator(() => input.iterator)
      .via(ledger.commandClient.submissionFlow())
      .take(take)
      .runWith(Sink.seq)

  private def toFuture[A](o: Option[A]): Future[A] = o match {
    case None => Future.failed(new IllegalStateException(s"empty option: $o"))
    case Some(x) => Future.successful(x)
  }

  private def ledgerEnd(ledger: LedgerClient): Future[LedgerOffset] =
    ledger.transactionClient.getLedgerEnd().flatMap(response => toFuture(response.offset))

  private def createCallablePayout(
      ledger: LedgerClient,
      from: P.Party,
      to: P.Party,
      contextId: TestContext,
      workflowId: WorkflowId,
      commandId: CommandId,
  ): Future[Seq[Ctx[TestContext, Try[Empty]]]] =
    send(
      ledger,
      contextId,
      aCommandSubmission(
        ledger,
        workflowId,
        commandId,
        from,
        CallablePayout(giver = from, receiver = to).create,
      ),
    )(1)

  private def exerciseCall(ledger: LedgerClient, party: P.Party, transaction: Transaction)(
      contextId: TestContext,
      workflowId: WorkflowId,
      commandId: CommandId,
  ): Future[Seq[Ctx[TestContext, Try[Empty]]]] =
    for {
      event <- toFuture(transaction.events.headOption)
      created <- toFuture(event.event.created)
      contractId = P.ContractId[CallablePayout](created.contractId)
      exerciseCommand = contractId.exerciseCall2()
      status <- send(
        ledger,
        contextId,
        aCommandSubmission(ledger, workflowId, commandId, party, exerciseCommand),
      )(1)
    } yield status

  private def exerciseTransfer(
      ledger: LedgerClient,
      party: P.Party,
      newReceiver: P.Party,
      transaction: Transaction,
  )(
      contextId: TestContext,
      workflowId: WorkflowId,
      commandId: CommandId,
  ): Future[Seq[Ctx[TestContext, Try[Empty]]]] =
    for {
      event <- toFuture(transaction.events.headOption)
      created <- toFuture(event.event.created)
      contractId = P.ContractId[CallablePayout](created.contractId)
      exerciseCommand = contractId.exerciseTransfer(newReceiver = newReceiver)
      status <- send(
        ledger,
        contextId,
        aCommandSubmission(ledger, workflowId, commandId, party, exerciseCommand),
      )(1)
    } yield status

  private def assertTransaction(
      tx: Transaction
  )(expectedCommandId: CommandId, expectedWorkflowId: WorkflowId): Assertion = {
    tx.commandId shouldBe expectedCommandId
    tx.workflowId shouldBe expectedWorkflowId
  }

  private def assertCreateEvent(
      event: Event
  )(expectedContract: Template[AnyRef], expectedAgreement: Option[String]): Assertion = {
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

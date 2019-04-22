// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import java.io.File
import java.time.Instant

import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.daml.lf.data.Ref.{QualifiedName, SimpleString, TypeConName}
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.Event.Events
import com.digitalasset.daml.lf.engine._
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.{GenTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, ExercisedEvent}
import com.digitalasset.ledger.api.v1.transaction.TreeEvent
import com.digitalasset.ledger.api.v1.value.Value.Sum.ContractId
import com.digitalasset.ledger.api.v1.value.{Identifier, Record, RecordField, Value, Variant}
import com.digitalasset.ledger.api.validation.CommandSubmissionRequestValidator
import com.digitalasset.platform.common.PlatformTypes.asVersionedValueOrThrow
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.sandbox.damle.SandboxDamle
import com.digitalasset.platform.sandbox.services.TestCommands
import com.digitalasset.platform.sandbox.stores.ActiveContractsInMemory
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.EventId
import com.digitalasset.platform.sandbox.{TestDar, TestHelpers}
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.server.services.transaction.{
  TransactionConversion,
  TransactionTreeNodes
}
import com.google.protobuf.empty.Empty
import org.scalatest.{Inside, Matchers, WordSpec}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.implicitConversions

@SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
class EventConverterSpec
    extends WordSpec
    with Matchers
    with TestHelpers
    with AkkaBeforeAndAfterAll
    with Inside {
  implicit def qualifiedNameStr(s: String): QualifiedName = QualifiedName.assertFromString(s)
  implicit def simpleStr(s: String): SimpleString = SimpleString.assertFromString(s)

  type LfTx = com.digitalasset.daml.lf.transaction.Transaction.Transaction

  implicit val ec: ExecutionContext = system.dispatcher

  private val etc = TransactionConversion

  private val testTemplate =
    Identifier(TestDar.parsedPackageId, moduleName = "Test", entityName = "Dummy")

  private val partyValue = Value(Value.Sum.Party("Alice"))
  private val fields: Seq[RecordField] = Seq(
    RecordField("operator", Some(partyValue))
  )
  private val args = Record(None, fields)
  private val create: Command = Command()
    .withCreate(
      CreateCommand()
        .withTemplateId(testTemplate)
        .withCreateArguments(args))

  private val ledgerId = "ledgerId"

  private val validator =
    new CommandSubmissionRequestValidator(
      ledgerId,
      IdentifierResolver(_ => Future.successful(None)))

  private val commands = Commands()
    .withParty("Alice")
    .withLedgerId(ledgerId)
    .withCommandId("cmdId")
    .withLedgerEffectiveTime(fromInstant(Instant.now()))
    .withMaximumRecordTime(fromInstant(Instant.now()))
    .withWorkflowId("workflowId")
    .withApplicationId("appId")
    .withCommands(Seq(create))

  object CommandsToTest extends TestCommands {
    override protected def darFile: File = new File("ledger/sandbox/Test.dar")

    val damlPackageContainer = DamlPackageContainer(scala.collection.immutable.List(darFile), true)
    val onKbCmd = oneKbCommandRequest("ledgerId", "big").getCommands
    val dummies = dummyCommands("ledgerId", "dummies").getCommands
    val paramShowcaseCreate = paramShowcase
  }

  val engine = Engine()

  "An interpreted commands" should {
    for ((testName, expectedSize, commands) <- List(
        ("large", 1, CommandsToTest.onKbCmd),
        ("dummies", 3, CommandsToTest.dummies),
        ("param showcase create", 1, CommandsToTest.paramShowcaseCreate))) {
      s"transform daml-lf views to events $testName" in {

        validator
          .validateCommands(commands)
          .map(validatedCommands =>
            for {
              tx <- Await.result(
                SandboxDamle.consume(engine.submit(validatedCommands.commands))(
                  damlPackageContainer,
                  contractLookup(ActiveContractsInMemory.empty),
                  keyLookup(ActiveContractsInMemory.empty)),
                5.seconds
              )
              blinding <- Blinding
                .checkAuthorizationAndBlind(tx, Set(commands.party))
            } yield {
              val txId = "testTx"
              val absCoid: Lf.ContractId => Lf.AbsoluteContractId =
                SandboxEventIdFormatter.makeAbsCoid(txId)
              val recordTr = tx
                .mapNodeId(SandboxEventIdFormatter.fromTransactionId(txId, _))
                .mapContractIdAndValue(absCoid, _.mapContractId(absCoid))
              val recordBlinding = blinding.explicitDisclosure.map {
                case (nid, parties) =>
                  (SandboxEventIdFormatter.fromTransactionId(txId, nid), parties)
              }

              val eventsFromViews =
                etc.genToApiTransaction(
                  recordTr,
                  recordBlinding.mapValues(_.map(_.underlyingString)))

              eventsFromViews.eventsById should have size expectedSize.toLong
          })
      }
    }

    "should generate the right event for a create" in {
      validator
        .validateCommands(commands)
        .map(validatedCommands =>
          for {
            tx <- Await.result(
              SandboxDamle.consume(engine.submit(validatedCommands.commands))(
                damlPackageContainer,
                contractLookup(ActiveContractsInMemory.empty),
                keyLookup(ActiveContractsInMemory.empty)),
              5.seconds
            )
            blinding <- Blinding
              .checkAuthorizationAndBlind(tx, Set(commands.party))
          } yield {
            val txId = "testTx"
            val absCoid: Lf.ContractId => Lf.AbsoluteContractId =
              SandboxEventIdFormatter.makeAbsCoid(txId)
            val recordTr = tx
              .mapNodeId(SandboxEventIdFormatter.fromTransactionId(txId, _))
              .mapContractIdAndValue(absCoid, _.mapContractId(absCoid))
            val recordBlinding = blinding.explicitDisclosure.map {
              case (nid, parties) => (SandboxEventIdFormatter.fromTransactionId(txId, nid), parties)
            }

            val eventsFromViews =
              etc.genToApiTransaction(
                recordTr,
                recordBlinding.mapValues(_.map(_.underlyingString)),
                true)

            tx.nodes.size shouldBe 1
            eventsFromViews.eventsById.size shouldBe 1
            eventsFromViews.eventsById.values.headOption
              .flatMap(_.getCreated.witnessParties.headOption) shouldBe Some("Alice")
            eventsFromViews.eventsById.values.headOption
              .flatMap(_.getCreated.getCreateArguments.fields.headOption) shouldBe Some(
              RecordField("operator", Some(partyValue))
            )
        })
    }

    "should generate the right event for an exercise" in {

      val ty = TypeConName("unimportant", "in.this:test")

      val nod0: (
          Transaction.NodeId,
          NodeExercises[
            Transaction.NodeId,
            Lf.AbsoluteContractId,
            Lf.VersionedValue[Lf.AbsoluteContractId]]) =
        (
          Transaction.NodeId.unsafeFromIndex(0),
          NodeExercises(
            Lf.AbsoluteContractId("someId"),
            ty,
            "Choice",
            None,
            consuming = true,
            Set("Alice"),
            asVersionedValueOrThrow(Lf.ValueUnit),
            Set("Alice"),
            Set("Alice"),
            Set("Alice"),
            ImmArray(Transaction.NodeId.unsafeFromIndex(1))
          ))

      val node1 = (
        Transaction.NodeId.unsafeFromIndex(1),
        NodeCreate(
          Lf.AbsoluteContractId("00"),
          Lf.ContractInst(
            ty,
            asVersionedValueOrThrow(
              Lf.ValueRecord(
                Some(Ref.Identifier("unimportant", QualifiedName.assertFromString("in.this:test"))),
                ImmArray(
                  (Some("field"), Lf.ValueText("someText")),
                  (
                    Some("field2"),
                    Lf.ValueVariant(
                      None,
                      "variant",
                      Lf.ValueRecord(None, ImmArray((Some("nested"), Lf.ValueInt64(100))))))
                )
              )),
            ""
          ),
          None,
          Set("Alice"),
          Set("Alice"),
          None
        ))

      val nodes: Map[
        Transaction.NodeId,
        GenNode[
          Transaction.NodeId,
          Lf.AbsoluteContractId,
          Lf.VersionedValue[Lf.AbsoluteContractId]]] =
        Seq(nod0, node1).toMap
      val tx: LfTx = GenTransaction(nodes, ImmArray(Transaction.NodeId.unsafeFromIndex(0)))
      val blinding = Blinding.blind(tx)
      val absCoid: Lf.ContractId => Lf.AbsoluteContractId =
        SandboxEventIdFormatter.makeAbsCoid("transactionId")
      val recordTx = tx
        .mapNodeId(SandboxEventIdFormatter.fromTransactionId("transactionId", _))
        .mapContractIdAndValue(absCoid, _.mapContractId(absCoid))
      val recordBlinding = blinding.explicitDisclosure.map {
        case (nid, parties) =>
          (SandboxEventIdFormatter.fromTransactionId("transactionId", nid), parties)
      }

      val events =
        etc.genToApiTransaction(
          recordTx,
          recordBlinding.mapValues(_.map(_.underlyingString)),
          verbose = true)

      val exercise = events.rootEventIds.headOption.map(events.eventsById).map(_.getExercised)
      val child =
        exercise.flatMap(_.childEventIds.headOption.map(events.eventsById).map(_.getCreated))
      val field1 = RecordField("field", Some(Value(Value.Sum.Text("someText"))))
      val nestedRecord =
        Some(
          Value(Value.Sum.Record(
            Record(None, Seq(RecordField("nested", Some(Value(Value.Sum.Int64(100)))))))))
      val field2 = RecordField(
        "field2",
        Some(Value(Value.Sum.Variant(Variant(None, "variant", nestedRecord)))))

      exercise.flatMap(_.witnessParties.headOption) shouldBe Some("Alice")

      exercise.map(_.contractId) shouldBe Some("someId")
      child.map(_.getTemplateId) shouldBe Some(
        Identifier("unimportant", "in.this.test", "in.this", "test"))
      child.map(_.getCreateArguments.fields) shouldBe Some(
        Vector(
          field1,
          field2
        ))

    }

    "nested exercises" in {
      val rootEx = ExerciseEvent(
        Lf.AbsoluteContractId("#5:1"),
        Ref.Identifier(
          "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
          "Test:Agreement"),
        "AcceptTriProposal",
        asVersionedValueOrThrow(
          Lf.ValueRecord(
            Some(
              Ref.Identifier(
                "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
                "Test:Agreement.AcceptTriProposal")),
            ImmArray((Some("cid"), Lf.ValueContractId(Lf.AbsoluteContractId("#6:0"))))
          )),
        Set("giver"),
        true,
        ImmArray("#txId:2"),
        Set("giver", "receiver"),
        Set("giver", "receiver")
      )
      val events: Events[EventId, Lf.AbsoluteContractId, Lf.VersionedValue[Lf.AbsoluteContractId]] =
        Events(
          ImmArray("#txId:0"),
          Map(
            "#txId:2" -> ExerciseEvent(
              Lf.AbsoluteContractId("#6:0"),
              Ref.Identifier(
                "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
                "Test:TriProposal"),
              "Accept",
              asVersionedValueOrThrow(Lf.ValueUnit),
              Set("receiver", "giver"),
              true,
              ImmArray("#txId:3"),
              Set("receiver", "giver", "operator"),
              Set("receiver", "giver", "operator")
            ),
            "#txId:3" -> CreateEvent(
              Lf.AbsoluteContractId("#txId:3"),
              Ref.Identifier(
                "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
                "Test:TriAgreement"),
              asVersionedValueOrThrow(Lf.ValueRecord(
                Some(Ref.Identifier(
                  "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
                  "Test:TriAgreement")),
                ImmArray(
                  (Some("operator"), Lf.ValueParty("operator")),
                  (Some("receiver"), Lf.ValueParty("receiver")),
                  (Some("giver"), Lf.ValueParty("giver")))
              )),
              Set("operator", "receiver", "giver"),
              Set("operator", "receiver", "giver")
            ),
            "#txId:0" -> rootEx
          )
        )

      val apiEvent =
        etc.eventsToTransaction(events, true)

      val created = CreatedEvent(
        "#txId:3",
        "#txId:3",
        Some(
          Identifier(
            "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
            name = "Test.TriAgreement",
            moduleName = "Test",
            entityName = "TriAgreement")),
        Some(
          Record(
            Some(
              Identifier(
                "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
                name = "Test.TriAgreement",
                moduleName = "Test",
                entityName = "TriAgreement")),
            Vector(
              RecordField("operator", Some(Value(Value.Sum.Party("operator")))),
              RecordField("receiver", Some(Value(Value.Sum.Party("receiver")))),
              RecordField("giver", Some(Value(Value.Sum.Party("giver"))))
            )
          )),
        Vector("operator", "receiver", "giver")
      )
      val nestedExercise = ExercisedEvent(
        "#txId:2",
        "#6:0",
        Some(
          Identifier(
            "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
            name = "Test.TriProposal",
            moduleName = "Test",
            entityName = "TriProposal")),
        "#6:0",
        "Accept",
        Some(Value(Value.Sum.Unit(Empty()))),
        Vector("receiver", "giver"),
        consuming = true,
        Vector("receiver", "giver", "operator"),
        List(created.eventId)
      )
      val topLevelExercise = ExercisedEvent(
        "#txId:0",
        "#5:1",
        Some(
          Identifier(
            "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
            name = "Test.Agreement",
            moduleName = "Test",
            entityName = "Agreement")),
        "#5:1",
        "AcceptTriProposal",
        Some(
          Value(Value.Sum.Record(Record(
            Some(Identifier(
              "0d25e199ed26977b3082864c62f8d154ca6042ed521712e2b3eb172dc79c87a2",
              name = "Test.Agreement.AcceptTriProposal",
              moduleName = "Test",
              entityName = "Agreement.AcceptTriProposal"
            )),
            Vector(RecordField("cid", Some(Value(ContractId("#6:0")))))
          )))),
        Vector("giver"),
        consuming = true,
        Vector("giver", "receiver"),
        List(nestedExercise.eventId)
      )

      val expected = TransactionTreeNodes(
        Map(
          created.eventId -> TreeEvent(TreeEvent.Kind.Created(created)),
          nestedExercise.eventId -> TreeEvent(TreeEvent.Kind.Exercised(nestedExercise)),
          topLevelExercise.eventId -> TreeEvent(TreeEvent.Kind.Exercised(topLevelExercise))
        ),
        List(topLevelExercise.eventId)
      )

      apiEvent.rootEventIds shouldEqual expected.rootEventIds
      apiEvent.eventsById.keys shouldEqual expected.eventsById.keys
      apiEvent.eventsById.keys.foreach { k =>
        inside(k) {
          case _ => apiEvent.eventsById(k) shouldEqual expected.eventsById(k)
        }
      }

    }
  }

  private def contractLookup(ac: ActiveContractsInMemory)(c: AbsoluteContractId) = {
    Future.successful(ac.contracts.get(c).map(_.contract))
  }

  private def keyLookup(ac: ActiveContractsInMemory)(gk: GlobalKey) =
    Future.successful(ac.keys.get(gk))
}

// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.{value => v}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.LedgerString
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.platform.store.dao.events.TransactionConversion
import com.daml.platform.store.dao.events.TransactionConversion.removeTransient
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

final class TransactionConversionSpec extends AnyWordSpec with Matchers {

  import TransactionBuilder.Implicits._

  private val List(contractId1, contractId2) = List.fill(2)(TransactionBuilder.newCid)

  private def create(contractId: Value.ContractId): Event =
    Event.of(
      Event.Event.Created(
        CreatedEvent("", contractId.coid, None, None, None, Seq.empty, Seq.empty, Seq.empty, None)
      )
    )

  private val create1 = create(contractId1)
  private val create2 = create(contractId2)
  private val archive1 = Event.of(
    Event.Event.Archived(ArchivedEvent("", contractId1.coid, None, Seq.empty))
  )

  "removeTransient" should {

    "remove Created and Archived events for the same contract from the transaction" in {
      removeTransient(Vector(create1, archive1)) shouldEqual Nil
    }

    "do not touch events with different contract identifiers" in {
      val events = Vector(create2, archive1)
      removeTransient(events) shouldBe events
    }

    "do not touch individual Created events" in {
      val events = Vector(create1)
      removeTransient(events) shouldEqual events
    }

    "do not touch individual Archived events" in {
      val events = Vector(archive1)
      removeTransient(events) shouldEqual events
    }
  }
  "ledgerEntryToTransactionTree" should {
    val party = "Alice"
    val offset = LedgerOffset.Absolute(LedgerString.assertFromString("offset"))

    def create(builder: TransactionBuilder, id: Value.ContractId) =
      builder.create(
        id = id,
        templateId = "M:T",
        argument = Value.ValueRecord(None, ImmArray.Empty),
        signatories = Set(party),
        observers = Set.empty,
        key = None,
      )

    def exercise(builder: TransactionBuilder, id: Value.ContractId) = {
      builder.exercise(
        contract = create(builder, id),
        choice = "C",
        consuming = true,
        actingParties = Set(party),
        argument = Value.ValueRecord(None, ImmArray.Empty),
      )
    }

    def createdEv(evId: String, contractId: String) =
      TreeEvent(
        TreeEvent.Kind.Created(
          CreatedEvent(
            eventId = s"#transactionId:$evId",
            contractId = contractId,
            templateId = Some(v.Identifier(defaultPackageId, "M", "T")),
            createArguments = Some(v.Record(None, Vector())),
            witnessParties = Vector(party),
            signatories = Vector(party),
            agreementText = Some(""),
          )
        )
      )

    def exercisedEv(evId: String, contractId: String, children: Seq[String]) =
      TreeEvent(
        TreeEvent.Kind.Exercised(
          ExercisedEvent(
            eventId = s"#transactionId:$evId",
            contractId = contractId,
            templateId = Some(v.Identifier(defaultPackageId, "M", "T")),
            interfaceId = None,
            choice = "C",
            choiceArgument = Some(v.Value(v.Value.Sum.Record(v.Record(None, Vector())))),
            actingParties = Vector(party),
            consuming = true,
            witnessParties = Vector(party),
            childEventIds = children.map(c => s"#transactionId:$c"),
          )
        )
      )

    def toEntry(transaction: CommittedTransaction) =
      LedgerEntry.Transaction(
        commandId = None,
        transactionId = Ref.TransactionId.assertFromString("transactionId"),
        applicationId = None,
        submissionId = Some(Ref.SubmissionId.assertFromString("submissionId")),
        actAs = List(party),
        workflowId = None,
        ledgerEffectiveTime = Timestamp.Epoch,
        recordedAt = Timestamp.Epoch,
        transaction = transaction,
        explicitDisclosure = Map.empty,
      )

    "remove rollback nodes" in {
      def cid(s: String): String = Value.ContractId.V1(Hash.hashPrivateKey(s)).coid
      val builder = TransactionBuilder()
      builder.add(create(builder, cid("#1")))
      val rollbackParent = builder.add(exercise(builder, cid("#0")))
      val rollback = builder.add(builder.rollback(), rollbackParent)
      builder.add(create(builder, cid("#2")), rollback)
      builder.add(exercise(builder, cid("#1")), rollback)
      val ex = builder.add(exercise(builder, cid("#1")))
      builder.add(create(builder, cid("#3")), ex)
      val transactionEntry = toEntry(builder.buildCommitted())
      TransactionConversion
        .ledgerEntryToTransactionTree(
          offset = offset,
          entry = transactionEntry,
          requestingParties = Set(party),
          verbose = false,
        )
        .get
        .eventsById shouldBe Map(
        "#transactionId:0" -> createdEv("0", cid("#1")),
        "#transactionId:1" -> exercisedEv("1", cid("#0"), Seq.empty),
        "#transactionId:5" -> exercisedEv("5", cid("#1"), Seq("6")),
        "#transactionId:6" -> createdEv("6", cid("#3")),
      )
    }
  }
}

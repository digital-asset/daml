// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{LedgerString, Party}
import com.daml.lf.transaction.CommittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.ledger.TransactionId
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.ledger.api.v1.event.{ArchivedEvent, CreatedEvent, Event, ExercisedEvent}
import com.daml.platform.index.TransactionConversion.removeTransient
import com.daml.platform.store.entries.LedgerEntry
import java.time.Instant
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.{value => v}

final class TransactionConversionSpec extends AnyWordSpec with Matchers {

  private val contractId1 = Value.ContractId.assertFromString("#contractId")
  private val contractId2 = Value.ContractId.assertFromString("#contractId2")
  private def create(contractId: Value.ContractId): Event =
    Event(
      Event.Event.Created(
        CreatedEvent("", contractId.coid, None, None, None, Seq.empty, Seq.empty, Seq.empty, None)
      )
    )

  private val create1 = create(contractId1)
  private val create2 = create(contractId2)
  private val archive1 = Event(
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
    val partyStr = "Alice"
    val party = Party.assertFromString(partyStr)
    val offset = LedgerOffset.Absolute(LedgerString.assertFromString("offset"))

    def create(builder: TransactionBuilder, id: String) =
      builder.create(
        id = id,
        template = "pkgid:M:T",
        argument = Value.ValueRecord(None, ImmArray.empty),
        signatories = Seq(partyStr),
        observers = Seq(),
        key = None,
      )

    def exercise(builder: TransactionBuilder, id: String) = {
      builder.exercise(
        contract = create(builder, id),
        choice = "C",
        consuming = true,
        actingParties = Set(partyStr),
        argument = Value.ValueRecord(None, ImmArray.empty),
      )
    }

    def createdEv(evId: String, contractId: String) =
      TreeEvent(
        TreeEvent.Kind.Created(
          CreatedEvent(
            eventId = s"#transactionId:$evId",
            contractId = contractId,
            templateId = Some(v.Identifier("pkgid", "M", "T")),
            createArguments = Some(v.Record(None, Vector())),
            witnessParties = Vector(partyStr),
            signatories = Vector(partyStr),
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
            templateId = Some(v.Identifier("pkgid", "M", "T")),
            choice = "C",
            choiceArgument = Some(v.Value(v.Value.Sum.Record(v.Record(None, Vector())))),
            actingParties = Vector(partyStr),
            consuming = true,
            witnessParties = Vector(partyStr),
            childEventIds = children.map(c => s"#transactionId:$c"),
          )
        )
      )

    def toEntry(transaction: CommittedTransaction) =
      LedgerEntry.Transaction(
        commandId = None,
        transactionId = TransactionId.assertFromString("transactionId"),
        applicationId = None,
        actAs = List(party),
        workflowId = None,
        ledgerEffectiveTime = Instant.EPOCH,
        recordedAt = Instant.EPOCH,
        transaction = transaction,
        explicitDisclosure = Map.empty,
      )

    "remove rollback nodes" in {
      val builder = TransactionBuilder()
      builder.add(create(builder, "#1"))
      val rollback = builder.add(builder.rollback())
      builder.add(create(builder, "#2"), rollback)
      builder.add(exercise(builder, "#1"), rollback)
      val ex = builder.add(exercise(builder, "#1"))
      builder.add(create(builder, "#3"), ex)
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
        "#transactionId:0" -> createdEv("0", "#1"),
        "#transactionId:4" -> exercisedEv("4", "#1", Seq("5")),
        "#transactionId:5" -> createdEv("5", "#3"),
      )
    }
  }
}

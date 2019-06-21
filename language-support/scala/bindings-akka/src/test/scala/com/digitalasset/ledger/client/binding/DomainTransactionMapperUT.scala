// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import java.time.Instant

import akka.stream.scaladsl.Source
import com.digitalasset.ledger.api.refinements.ApiTypes._
import com.digitalasset.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.digitalasset.ledger.api.v1.event._
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value.Absolute
import com.digitalasset.ledger.api.v1.transaction.Transaction
import com.digitalasset.ledger.api.v1.value.{Identifier, Record}
import com.digitalasset.ledger.client.testing.AkkaTest
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.{Matchers, WordSpec}

import scala.collection.immutable

class DomainTransactionMapperUT extends WordSpec with Matchers with AkkaTest {
  private val mockContract = Contract(Primitive.ContractId("contractId"), MockTemplate(), None, Seq.empty, Seq.empty)
  private val transactionMapper = new DomainTransactionMapper(createdEvent => Right(mockContract))

  private def getResult(source: immutable.Iterable[Transaction]): Seq[DomainTransaction] =
    drain(
      Source(source)
        .via(transactionMapper.transactionsMapper)
    )

  def createdEvent(contractId: String) =
    Event(
      Created(
        CreatedEvent(
          "createdEventId",
          contractId,
          Some(Identifier("createdTemplateId")),
          None,
          Some(Record()))))

  def archivedEvent(contractId: String) =
    Event(
      Archived(
        ArchivedEvent("archivedEventId", contractId, Some(Identifier("archivedTemplateId")))))

  def domainCreatedEvent(contractId: String) =
    DomainCreatedEvent(
      EventId("createdEventId"),
      ContractId(contractId),
      TemplateId(Identifier("createdTemplateId")),
      List.empty,
      CreateArguments(Record()),
      mockContract
    )

  def domainArchivedEvent(contractId: String) =
    DomainArchivedEvent(
      EventId("archivedEventId"),
      ContractId(contractId),
      TemplateId(Identifier("archivedTemplateId")),
      List.empty)

  case class MockTemplate() extends Template[MockTemplate] {
    override protected[this] def templateCompanion(
        implicit d: DummyImplicit): TemplateCompanion[MockTemplate] =
      new TemplateCompanion.Empty[MockTemplate] {
        override val onlyInstance = MockTemplate()
        override val id: Primitive.TemplateId[MockTemplate] =
          ` templateId`("packageId", "moduleId", "templateId")
        override val consumingChoices: Set[Choice] = Set.empty
      }
  }

  private val now = Instant.now()
  private val time = Timestamp(now.getEpochSecond, now.getNano)

  private def transaction(events: Event*) =
    Transaction("tid", "cid", "wid", Some(time), events, "0")

  private def domainTransaction(events: DomainEvent*) =
    DomainTransaction(
      TransactionId("tid"),
      WorkflowId("wid"),
      LedgerOffset(Absolute("0")),
      CommandId("cid"),
      time,
      events,
      None)

  "DomainTransactionMapper" should {
    "should map events to domain events" in {
      val result = getResult(
        List(
          transaction(createdEvent("1")),
          transaction(createdEvent("2")),
          transaction(archivedEvent("2")),
        )
      )
      result should contain theSameElementsInOrderAs List(
        domainTransaction(domainCreatedEvent("1")),
        domainTransaction(domainCreatedEvent("2")),
        domainTransaction(domainArchivedEvent("2")),
      )
    }
  }
}

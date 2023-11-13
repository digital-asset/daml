// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import java.time.Instant

import org.apache.pekko.stream.scaladsl.Source
import com.daml.ledger.api.refinements.ApiTypes._
import com.daml.ledger.api.v1.event.Event.Event.{Archived, Created}
import com.daml.ledger.api.v1.event._
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.Value.Absolute
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.ledger.client.testing.PekkoTest
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable

class DomainTransactionMapperUT extends AnyWordSpec with Matchers with PekkoTest {
  private val mockContract =
    Contract(Primitive.ContractId("contractId"), MockTemplate(), None, Seq.empty, Seq.empty, None)
  private val transactionMapper = new DomainTransactionMapper(_ => Right(mockContract))

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
          Some(Identifier("pkgId", "modName", "createdTemplateId")),
          None,
          Some(Record()),
        )
      )
    )

  def archivedEvent(contractId: String) =
    Event(
      Archived(
        ArchivedEvent(
          "archivedEventId",
          contractId,
          Some(Identifier("pkgId", "modName", "archivedTemplateId")),
        )
      )
    )

  def domainCreatedEvent(contractId: String) =
    DomainCreatedEvent(
      EventId("createdEventId"),
      ContractId(contractId),
      TemplateId(Identifier("pkgId", "modName", "createdTemplateId")),
      List.empty,
      CreateArguments(Record()),
      mockContract,
    )

  def domainArchivedEvent(contractId: String) =
    DomainArchivedEvent(
      EventId("archivedEventId"),
      ContractId(contractId),
      TemplateId(Identifier("pkgId", "modName", "archivedTemplateId")),
      List.empty,
    )

  case class MockTemplate() extends Template[MockTemplate] {
    override protected[this] def templateCompanion(implicit
        d: DummyImplicit
    ): TemplateCompanion[MockTemplate] =
      new TemplateCompanion.Empty[MockTemplate] {
        override val onlyInstance = MockTemplate()
        override val id: Primitive.TemplateId[MockTemplate] =
          ` templateId`("packageId", "modName", "templateId")
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
    )

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

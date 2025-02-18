// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  TemplatePartiesFilter,
}
import com.digitalasset.daml.lf.data.Ref.Party
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

private[dao] trait JdbcLedgerDaoEventsSpec extends LoneElement with Inside with OptionValues {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  private def eventsReader = ledgerDao.eventsReader

  behavior of "JdbcLedgerDao (events)"

  it should "lookup a create event by contract id" in {

    for {
      (_, tx) <- store(singleCreate(cId => create(cId)))
      flatTx <- ledgerDao.updateReader.lookupTransactionById(
        tx.updateId,
        transactionFormatForWildcardParties(tx.actAs.toSet),
      )
      result <- eventsReader.getEventsByContractId(
        nonTransient(tx).loneElement,
        Set(alice),
      )
    } yield {
      val expected = flatTx.value.transaction.value.events.loneElement.event.created.value
      val actual = result.created.flatMap(_.createdEvent).value
      actual shouldBe expected
    }
  }

  it should "lookup an archive event by contract id" in {
    for {
      (_, tx1) <- store(singleCreate(cId => create(cId)))
      contractId = nonTransient(tx1).loneElement
      (_, tx2) <- store(singleExercise(contractId))
      flatTx <- ledgerDao.updateReader.lookupTransactionById(
        tx2.updateId,
        transactionFormatForWildcardParties(tx2.actAs.toSet),
      )
      expected = flatTx.value.transaction.value.events.loneElement.event.archived.value
      result <- eventsReader.getEventsByContractId(contractId, Set(alice))
    } yield {
      val actual = result.archived.flatMap(_.archivedEvent).value
      actual shouldBe expected
    }
  }

  it should "make events visible to signatories and observers (stakeholders) only" in {

    for {
      (_, tx) <- store(
        singleCreate(cId => create(cId, signatories = Set(alice), observers = Set(charlie)))
      )
      _ <- ledgerDao.updateReader.lookupTransactionTreeById(
        tx.updateId,
        tx.actAs.toSet,
      )
      cId = nonTransient(tx).loneElement
      aliceView <- eventsReader.getEventsByContractId(cId, Set(alice))
      charlieView <- eventsReader.getEventsByContractId(cId, Set(charlie))
      emmaView <- eventsReader.getEventsByContractId(cId, Set(emma))
    } yield {
      aliceView.created.flatMap(_.createdEvent).isDefined shouldBe true
      charlieView.created.flatMap(_.createdEvent).isDefined shouldBe true
      emmaView.created.flatMap(_.createdEvent).isDefined shouldBe false
    }
  }

  private def transactionFormatForWildcardParties(
      requestingParties: Set[Party]
  ): InternalTransactionFormat =
    InternalTransactionFormat(
      internalEventFormat = InternalEventFormat(
        templatePartiesFilter = TemplatePartiesFilter(
          relation = Map.empty,
          templateWildcardParties = Some(requestingParties),
        ),
        eventProjectionProperties = EventProjectionProperties(
          verbose = true,
          templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
        ),
      ),
      transactionShape = AcsDelta,
    )

}

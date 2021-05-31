// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.lf.data.Ref.Party
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.platform.participant.util.LfEngineToApi
import org.scalatest._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoActiveContractsSpec
    extends OptionValues
    with Inside
    with Inspectors
    with LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  behavior of "JdbcLedgerDao (getActiveContracts)"

  it should "serve the correct contracts after a series of transactions" in {
    for {
      before <- ledgerDao.lookupLedgerEnd()
      (_, t1) <- store(singleCreate)
      (_, t2) <- store(singleCreate)
      (_, _) <- store(singleExercise(nonTransient(t2).loneElement))
      (_, _) <- store(fullyTransient())
      (_, t5) <- store(singleCreate)
      (_, t6) <- store(singleCreate)
      after <- ledgerDao.lookupLedgerEnd()
      activeContractsBefore <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = before,
            filter = Map(alice -> Set.empty, bob -> Set.empty, charlie -> Set.empty),
            verbose = true,
          )
      )
      activeContractsAfter <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = after,
            filter = Map(alice -> Set.empty, bob -> Set.empty, charlie -> Set.empty),
            verbose = true,
          )
      )
    } yield {
      val activeContracts = activeContractsAfter.toSet.diff(activeContractsBefore.toSet)
      activeContracts should have size 3
      activeContracts.map(_.contractId) shouldBe Set(
        nonTransient(t1).loneElement.coid,
        nonTransient(t5).loneElement.coid,
        nonTransient(t6).loneElement.coid,
      )
    }
  }

  it should "serve a stable result based on the input offset" in {
    for {
      offset <- ledgerDao.lookupLedgerEnd()
      activeContractsBefore <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = offset,
            filter = Map(alice -> Set.empty, bob -> Set.empty, charlie -> Set.empty),
            verbose = true,
          )
      )
      (_, _) <- store(singleCreate)
      (_, c) <- store(singleCreate)
      (_, _) <- store(singleExercise(nonTransient(c).loneElement))
      (_, _) <- store(fullyTransient())
      (_, _) <- store(singleCreate)
      (_, _) <- store(singleCreate)
      activeContractsAfter <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = offset,
            filter = Map(alice -> Set.empty, bob -> Set.empty, charlie -> Set.empty),
            verbose = true,
          )
      )
    } yield {
      activeContractsAfter.toSet.diff(activeContractsBefore.toSet) should have size 0
    }
  }

  it should "filter correctly for a single party" in {
    val party1 = Party.assertFromString(UUID.randomUUID.toString)
    val party2 = Party.assertFromString(UUID.randomUUID.toString)
    for {
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            (party1, someTemplateId, someContractArgument),
            (party2, otherTemplateId, otherContractArgument),
            (party1, otherTemplateId, otherContractArgument),
          ),
        )
      )
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      result <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = ledgerEnd,
            filter = Map(party1 -> Set(otherTemplateId)),
            verbose = true,
          )
      )
    } yield {
      val create = result.loneElement
      create.witnessParties.loneElement shouldBe party1
      create.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
    }
  }

  it should "filter correctly by multiple parties with the same template" in {
    val party1 = Party.assertFromString(UUID.randomUUID.toString)
    val party2 = Party.assertFromString(UUID.randomUUID.toString)
    for {
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            (party1, someTemplateId, someContractArgument),
            (party2, otherTemplateId, otherContractArgument),
            (party1, otherTemplateId, otherContractArgument),
          ),
        )
      )
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      result <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = ledgerEnd,
            filter = Map(
              party1 -> Set(otherTemplateId),
              party2 -> Set(otherTemplateId),
            ),
            verbose = true,
          )
      )
    } yield {
      val activeContracts = result.toArray
      activeContracts should have length 2

      val create1 = activeContracts(0)
      create1.witnessParties.loneElement shouldBe party2
      create1.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)

      val create2 = activeContracts(1)
      create2.witnessParties.loneElement shouldBe party1
      create2.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
    }
  }

  it should "filter correctly by multiple parties with different templates" in {
    val party1 = Party.assertFromString(UUID.randomUUID.toString)
    val party2 = Party.assertFromString(UUID.randomUUID.toString)
    for {
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            (party1, someTemplateId, someContractArgument),
            (party2, otherTemplateId, otherContractArgument),
            (party1, otherTemplateId, otherContractArgument),
          ),
        )
      )
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      result <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = ledgerEnd,
            filter = Map(
              party1 -> Set(someTemplateId),
              party2 -> Set(otherTemplateId),
            ),
            verbose = true,
          )
      )
    } yield {
      val activeContracts = result.toArray
      activeContracts should have length 2

      val create2 = activeContracts(0)
      create2.witnessParties.loneElement shouldBe party1
      create2.templateId.value shouldBe LfEngineToApi.toApiIdentifier(someTemplateId)

      val create1 = activeContracts(1)
      create1.witnessParties.loneElement shouldBe party2
      create1.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
    }
  }

  it should "filter correctly by multiple parties with different template and wildcards" in {
    val party1 = Party.assertFromString(UUID.randomUUID.toString)
    val party2 = Party.assertFromString(UUID.randomUUID.toString)
    for {
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            (party1, someTemplateId, someContractArgument),
            (party2, otherTemplateId, otherContractArgument),
            (party1, otherTemplateId, otherContractArgument),
          ),
        )
      )
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      result <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = ledgerEnd,
            filter = Map(
              party1 -> Set(someTemplateId),
              party2 -> Set.empty,
            ),
            verbose = true,
          )
      )
    } yield {
      val activeContracts = result.toArray
      activeContracts should have length 2

      val create2 = activeContracts(0)
      create2.witnessParties.loneElement shouldBe party1
      create2.templateId.value shouldBe LfEngineToApi.toApiIdentifier(someTemplateId)

      val create1 = activeContracts(1)
      create1.witnessParties.loneElement shouldBe party2
      create1.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
    }
  }

  it should "not set the offset" in {
    for {
      (_, t1) <- store(singleCreate)
      (_, t2) <- store(singleCreate)
      end <- ledgerDao.lookupLedgerEnd()
      activeContracts <- ledgerDao.transactionsReader
        .getActiveContracts(
          activeAt = end,
          filter = Map(alice -> Set.empty),
          verbose = true,
        )
        .runWith(Sink.seq)

    } yield {
      activeContracts should not be empty
      forAll(activeContracts) { ac =>
        ac.offset shouldBe empty
      }
    }
  }

  private def activeContractsOf(
      source: Source[GetActiveContractsResponse, NotUsed]
  ): Future[Seq[CreatedEvent]] =
    source.runWith(Sink.seq).map(_.flatMap(_.activeContracts))

}

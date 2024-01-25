// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.lf.data.Ref.{Identifier, Party}
import com.digitalasset.canton.platform.TemplatePartiesFilter
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection
import com.digitalasset.canton.util.LfEngineToApi
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.*
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
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
            activeAt = before.lastOffset,
            filter = TemplatePartiesFilter(Map.empty, Set(alice, bob, charlie)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(alice, bob, charlie),
            ),
            multiDomainEnabled = false,
          )
      )
      activeContractsAfter <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = after.lastOffset,
            filter = TemplatePartiesFilter(Map.empty, Set(alice, bob, charlie)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(alice, bob, charlie),
            ),
            multiDomainEnabled = false,
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
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      offset = ledgerEnd.lastOffset
      activeContractsBefore <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = offset,
            filter = TemplatePartiesFilter(Map.empty, Set(alice, bob, charlie)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(alice, bob, charlie),
            ),
            multiDomainEnabled = false,
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
            filter = TemplatePartiesFilter(Map.empty, Set(alice, bob, charlie)),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(alice, bob, charlie),
            ),
            multiDomainEnabled = false,
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
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(Map(otherTemplateId -> Set(party1)), Set.empty),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set.empty,
              witnessTemplateProjections =
                Map(party1 -> Map(otherTemplateId -> Projection(contractArguments = true))),
            ),
            multiDomainEnabled = false,
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
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(
              Map(
                otherTemplateId -> Set(party1, party2)
              ),
              Set.empty,
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set.empty,
              witnessTemplateProjections = Map(
                party1 -> Map(otherTemplateId -> Projection(contractArguments = true)),
                party2 -> Map(otherTemplateId -> Projection(contractArguments = true)),
              ),
            ),
            multiDomainEnabled = false,
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
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(
              Map(
                someTemplateId -> Set(party1),
                otherTemplateId -> Set(party2),
              ),
              Set.empty,
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set.empty,
              witnessTemplateProjections = Map(
                party1 -> Map(otherTemplateId -> Projection(contractArguments = true)),
                party2 -> Map(otherTemplateId -> Projection(contractArguments = true)),
              ),
            ),
            multiDomainEnabled = false,
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
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(
              Map(
                someTemplateId -> Set(party1)
              ),
              Set(party2),
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(party2),
              witnessTemplateProjections = Map(
                party1 -> Map(someTemplateId -> Projection(contractArguments = true))
              ),
            ),
            multiDomainEnabled = false,
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

  it should "filter correctly with unknown parties and templates" in {
    val party1 = Party.assertFromString(UUID.randomUUID.toString)
    val party2 = Party.assertFromString(UUID.randomUUID.toString)

    // Adding an unknown party and/or template to the filter should not
    // affect the results
    val unknownParty = Party.assertFromString(UUID.randomUUID.toString)
    val unknownTemplate = Identifier.assertFromString("pkg:Mod:Template")

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
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(
              Map(
                someTemplateId -> Set(party1)
              ),
              Set(party2),
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(party2),
              witnessTemplateProjections = Map(
                party1 -> Map(someTemplateId -> Projection(contractArguments = true))
              ),
            ),
            multiDomainEnabled = false,
          )
      )
      resultUnknownParty <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(
              Map(
                someTemplateId -> Set(party1)
              ),
              Set(party2, unknownParty),
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(party2, unknownParty),
              witnessTemplateProjections = Map(
                party1 -> Map(someTemplateId -> Projection(contractArguments = true))
              ),
            ),
            multiDomainEnabled = false,
          )
      )
      resultUnknownTemplate <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(
              Map(
                someTemplateId -> Set(party1),
                unknownTemplate -> Set(party1),
              ),
              Set(party2),
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(party2),
              witnessTemplateProjections = Map(
                party1 -> Map(
                  someTemplateId -> Projection(contractArguments = true),
                  unknownTemplate -> Projection(contractArguments = true),
                )
              ),
            ),
            multiDomainEnabled = false,
          )
      )
      resultUnknownPartyAndTemplate <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(
              Map(
                someTemplateId -> Set(party1),
                unknownTemplate -> Set(party1),
              ),
              Set(party2, unknownParty),
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set(party2, unknownParty),
              witnessTemplateProjections = Map(
                party1 -> Map(
                  someTemplateId -> Projection(contractArguments = true),
                  unknownTemplate -> Projection(contractArguments = true),
                )
              ),
            ),
            multiDomainEnabled = false,
          )
      )
      resultUnknownsOnly <- activeContractsOf(
        ledgerDao.transactionsReader
          .getActiveContracts(
            activeAt = ledgerEnd.lastOffset,
            filter = TemplatePartiesFilter(
              Map(
                unknownTemplate -> Set(unknownParty)
              ),
              Set.empty,
            ),
            eventProjectionProperties = EventProjectionProperties(
              verbose = true,
              wildcardWitnesses = Set.empty,
              witnessTemplateProjections = Map(
                unknownParty -> Map(
                  unknownTemplate -> Projection(contractArguments = true)
                )
              ),
            ),
            multiDomainEnabled = false,
          )
      )
    } yield {
      result should have length 2
      resultUnknownParty should contain theSameElementsAs result
      resultUnknownTemplate should contain theSameElementsAs result
      resultUnknownPartyAndTemplate should contain theSameElementsAs result

      resultUnknownsOnly shouldBe empty
    }
  }

  it should "not set the offset" in {
    for {
      (_, t1) <- store(singleCreate)
      (_, t2) <- store(singleCreate)
      end <- ledgerDao.lookupLedgerEnd()
      activeContracts <- ledgerDao.transactionsReader
        .getActiveContracts(
          activeAt = end.lastOffset,
          filter = TemplatePartiesFilter(Map.empty, Set(alice)),
          eventProjectionProperties = EventProjectionProperties(verbose = true, Set(alice)),
          multiDomainEnabled = false,
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
    source.runWith(Sink.seq).map(_.flatMap(_.contractEntry.activeContract.flatMap(_.createdEvent)))
}

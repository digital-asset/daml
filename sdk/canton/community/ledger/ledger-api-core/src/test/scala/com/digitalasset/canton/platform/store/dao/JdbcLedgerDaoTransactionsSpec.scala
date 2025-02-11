// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.ledger.resources.ResourceContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.platform.store.dao.*
import com.digitalasset.canton.platform.store.entries.LedgerEntry
import com.digitalasset.canton.platform.store.utils.EventOps.EventOps
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
  TemplatePartiesFilter,
}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.Node
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalacheck.Gen
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, LoneElement, OptionValues}

import scala.concurrent.Future

private[dao] trait JdbcLedgerDaoTransactionsSpec extends OptionValues with Inside with LoneElement {
  this: AsyncFlatSpec with Matchers with JdbcLedgerDaoSuite =>

  import JdbcLedgerDaoTransactionsSpec.*

  behavior of "JdbcLedgerDao (lookupFlatTransactionById, lookupFlatTransactionByOffset)"

  it should "return nothing for a mismatching transaction id" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.updateReader
        .lookupTransactionById(
          updateId = "WRONG",
          internalTransactionFormat = transactionFormatForWildcardParties(tx.actAs.toSet),
        )
    } yield {
      result shouldBe None
    }
  }

  it should "return nothing for a mismatching offset" in {
    for {
      (_, tx) <- store(singleCreate)
      result <- ledgerDao.updateReader
        .lookupTransactionByOffset(
          offset = Offset.tryFromLong(12345678L),
          internalTransactionFormat = transactionFormatForWildcardParties(tx.actAs.toSet),
        )
    } yield {
      result shouldBe None
    }
  }

  it should "return nothing for a mismatching party" in {
    for {
      (offset, tx) <- store(singleCreate)
      resultById <- ledgerDao.updateReader
        .lookupTransactionById(tx.updateId, transactionFormatForWildcardParties(Set("WRONG")))
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionByOffset(offset, transactionFormatForWildcardParties(Set("WRONG")))
    } yield {
      resultById shouldBe None
      resultByOffset shouldBe resultById
    }
  }

  it should "return the expected flat transaction for a correct request (create)" in {
    for {
      (offset, tx) <- store(singleCreate)
      resultById <- ledgerDao.updateReader
        .lookupTransactionById(tx.updateId, transactionFormatForWildcardParties(tx.actAs.toSet))
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionByOffset(offset, transactionFormatForWildcardParties(tx.actAs.toSet))
    } yield {
      inside(resultById.value.transaction) { case Some(transaction) =>
        transaction.commandId shouldBe tx.commandId.value
        transaction.offset shouldBe offset.unwrap
        TimestampConversion.toLf(
          transaction.effectiveAt.value,
          TimestampConversion.ConversionMode.Exact,
        ) shouldBe tx.ledgerEffectiveTime
        transaction.updateId shouldBe tx.updateId
        transaction.workflowId shouldBe tx.workflowId.getOrElse("")
        inside(transaction.events.loneElement.event.created) { case Some(created) =>
          inside(tx.transaction.nodes.headOption) { case Some((nodeId, createNode: Node.Create)) =>
            created.offset shouldBe offset.unwrap
            created.nodeId shouldBe nodeId.index
            created.witnessParties should contain only (tx.actAs*)
            created.contractKey shouldBe None
            created.createArguments shouldNot be(None)
            created.signatories should contain theSameElementsAs createNode.signatories
            created.observers should contain theSameElementsAs createNode.stakeholders.diff(
              createNode.signatories
            )
            created.templateId shouldNot be(None)
          }
        }
      }
      resultByOffset shouldBe resultById
    }
  }

  it should "return the expected flat transaction for a correct request (exercise)" in {
    for {
      (_, create) <- store(singleCreate)
      (offset, exercise) <- store(singleExercise(nonTransient(create).loneElement))
      resultById <- ledgerDao.updateReader
        .lookupTransactionById(
          updateId = exercise.updateId,
          internalTransactionFormat = transactionFormatForWildcardParties(exercise.actAs.toSet),
        )
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionByOffset(
          offset = offset,
          internalTransactionFormat = transactionFormatForWildcardParties(exercise.actAs.toSet),
        )
    } yield {
      inside(resultById.value.transaction) { case Some(transaction) =>
        transaction.commandId shouldBe exercise.commandId.value
        transaction.offset shouldBe offset.unwrap
        transaction.updateId shouldBe exercise.updateId
        TimestampConversion.toLf(
          transaction.effectiveAt.value,
          TimestampConversion.ConversionMode.Exact,
        ) shouldBe exercise.ledgerEffectiveTime
        transaction.workflowId shouldBe exercise.workflowId.getOrElse("")
        inside(transaction.events.loneElement.event.archived) { case Some(archived) =>
          inside(exercise.transaction.nodes.headOption) {
            case Some((nodeId, exerciseNode: Node.Exercise)) =>
              archived.offset shouldBe offset.unwrap
              archived.nodeId shouldBe nodeId.index
              archived.witnessParties should contain only (exercise.actAs*)
              archived.contractId shouldBe exerciseNode.targetCoid.coid
              archived.templateId shouldNot be(None)
          }
        }
      }
      resultByOffset shouldBe resultById
    }
  }

  it should "show command IDs to the original submitters (lookupFlatTransactionById)" in {
    val signatories = Set(alice, bob)
    val stakeholders = Set(alice, bob, charlie) // Charlie is only stakeholder
    val actAs = List(alice, bob, david) // David is submitter but not signatory
    for {
      (_, tx) <- store(singleCreate(createNode(_, signatories, stakeholders), actAs))
      // Response 1: querying as all submitters
      result1 <- ledgerDao.updateReader
        .lookupTransactionById(
          updateId = tx.updateId,
          internalTransactionFormat = transactionFormatForWildcardParties(Set(alice, bob, david)),
        )
      // Response 2: querying as a proper subset of all submitters
      result2 <- ledgerDao.updateReader
        .lookupTransactionById(tx.updateId, transactionFormatForWildcardParties(Set(alice, david)))
      // Response 3: querying as a proper superset of all submitters
      result3 <- ledgerDao.updateReader
        .lookupTransactionById(
          updateId = tx.updateId,
          internalTransactionFormat =
            transactionFormatForWildcardParties(Set(alice, bob, charlie, david)),
        )
    } yield {
      result1.value.transaction.value.commandId shouldBe tx.commandId.value
      result2.value.transaction.value.commandId shouldBe tx.commandId.value
      result3.value.transaction.value.commandId shouldBe tx.commandId.value
    }
  }

  it should "show command IDs to the original submitters (lookupFlatTransactionByOffset)" in {
    val signatories = Set(alice, bob)
    val stakeholders = Set(alice, bob, charlie) // Charlie is only stakeholder
    val actAs = List(alice, bob, david) // David is submitter but not signatory
    for {
      (offset, tx) <- store(singleCreate(createNode(_, signatories, stakeholders), actAs))
      // Response 1: querying as all submitters
      result1 <- ledgerDao.updateReader
        .lookupTransactionByOffset(
          offset = offset,
          internalTransactionFormat = transactionFormatForWildcardParties(Set(alice, bob, david)),
        )
      // Response 2: querying as a proper subset of all submitters
      result2 <- ledgerDao.updateReader
        .lookupTransactionByOffset(offset, transactionFormatForWildcardParties(Set(alice, david)))
      // Response 3: querying as a proper superset of all submitters
      result3 <- ledgerDao.updateReader
        .lookupTransactionByOffset(
          offset = offset,
          internalTransactionFormat =
            transactionFormatForWildcardParties(Set(alice, bob, charlie, david)),
        )
    } yield {
      result1.value.transaction.value.commandId shouldBe tx.commandId.value
      result2.value.transaction.value.commandId shouldBe tx.commandId.value
      result3.value.transaction.value.commandId shouldBe tx.commandId.value
    }
  }

  it should "hide command IDs from non-submitters" in {
    val signatories = Set(alice, bob)
    val stakeholders = Set(alice, bob, charlie) // Charlie is only stakeholder
    val actAs = List(alice, bob, david) // David is submitter but not signatory
    for {
      (offset, tx) <- store(singleCreate(createNode(_, signatories, stakeholders), actAs))
      resultById <- ledgerDao.updateReader
        .lookupTransactionById(tx.updateId, transactionFormatForWildcardParties(Set(charlie)))
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionByOffset(offset, transactionFormatForWildcardParties(Set(charlie)))
    } yield {
      resultById.value.transaction.value.commandId shouldBe ""
      resultByOffset shouldBe resultById
    }
  }

  it should "hide events on transient contracts to the original submitter" in {
    for {
      (offset, tx) <- store(fullyTransient())
      resultById <- ledgerDao.updateReader
        .lookupTransactionById(tx.updateId, transactionFormatForWildcardParties(tx.actAs.toSet))
      resultByOffset <- ledgerDao.updateReader
        .lookupTransactionByOffset(offset, transactionFormatForWildcardParties(tx.actAs.toSet))
    } yield {
      inside(resultById.value.transaction) { case Some(transaction) =>
        transaction.commandId shouldBe tx.commandId.value
        transaction.offset shouldBe offset.unwrap
        transaction.updateId shouldBe tx.updateId
        TimestampConversion.toLf(
          transaction.effectiveAt.value,
          TimestampConversion.ConversionMode.Exact,
        ) shouldBe tx.ledgerEffectiveTime
        transaction.workflowId shouldBe tx.workflowId.getOrElse("")
        transaction.events shouldBe Seq.empty
      }
      resultByOffset shouldBe resultById
    }
  }

  behavior of "JdbcLedgerDao (getUpdates with AcsDelta)"

  it should "match the results of lookupFlatTransactionById" in {
    for {
      (from, to, transactions) <- storeTestFixture()
      lookups <- lookupIndividually(transactions, Set(alice, bob, charlie))
      result <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from,
            endInclusive = to,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(Map.empty, Some(Set(alice, bob, charlie))),
              eventProjectionProperties = EventProjectionProperties(
                verbose = true,
                templateWildcardWitnesses = Some(Set(alice, bob, charlie)),
              ),
            ),
          )
      )
    } yield {
      comparable(result) should contain theSameElementsInOrderAs comparable(lookups)
    }
  }

  it should "filter correctly by party" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, tx) <- store(
        multipleCreates(
          charlie,
          Seq(
            (alice, someTemplateId, someContractArgument),
            (bob, someTemplateId, someContractArgument),
          ),
        )
      )
      to <- ledgerDao.lookupLedgerEnd()
      individualLookupForAlice <- lookupIndividually(Seq(tx), as = Set(alice))
      individualLookupForBob <- lookupIndividually(Seq(tx), as = Set(bob))
      individualLookupForCharlie <- lookupIndividually(Seq(tx), as = Set(charlie))
      resultForAlice <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(Map.empty, Some(Set(alice))),
              eventProjectionProperties = EventProjectionProperties(
                verbose = true,
                templateWildcardWitnesses = Some(Set(alice)),
              ),
            ),
          )
      )
      resultForBob <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(Map.empty, Some(Set(bob))),
              eventProjectionProperties = EventProjectionProperties(
                verbose = true,
                templateWildcardWitnesses = Some(Set(bob)),
              ),
            ),
          )
      )
      resultForCharlie <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(Map.empty, Some(Set(charlie))),
              eventProjectionProperties = EventProjectionProperties(
                verbose = true,
                templateWildcardWitnesses = Some(Set(charlie)),
              ),
            ),
          )
      )
    } yield {
      individualLookupForAlice should contain theSameElementsInOrderAs resultForAlice
      individualLookupForBob should contain theSameElementsInOrderAs resultForBob
      individualLookupForCharlie should contain theSameElementsInOrderAs resultForCharlie
    }
  }

  it should "filter correctly for a single party" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            (alice, someTemplateId, someContractArgument),
            (bob, otherTemplateId, otherContractArgument),
            (alice, otherTemplateId, otherContractArgument),
          ),
        )
      )
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter =
                TemplatePartiesFilter(Map(otherTemplateId -> Some(Set(alice))), Some(Set.empty)),
              eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
            ),
          )
      )
    } yield {
      inside(result.loneElement.events.loneElement.event.created) { case Some(create) =>
        create.witnessParties.loneElement shouldBe alice
        create.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
      }
    }
  }

  it should "filter correctly by multiple parties with the same template" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            (alice, someTemplateId, someContractArgument),
            (bob, otherTemplateId, otherContractArgument),
            (alice, otherTemplateId, otherContractArgument),
          ),
        )
      )
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(
                relation = Map(
                  otherTemplateId -> Some(Set(alice, bob))
                ),
                templateWildcardParties = Some(Set.empty),
              ),
              eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
            ),
          )
      )
      resultPartyWildcard <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(
                relation = Map(otherTemplateId -> None),
                templateWildcardParties = Some(Set.empty),
              ),
              eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
            ),
          )
      )

    } yield {
      val events = result.loneElement.events.toArray
      events should have length 2
      inside(events(0).event.created) { case Some(create) =>
        create.witnessParties.loneElement shouldBe bob
        create.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
      }
      inside(events(1).event.created) { case Some(create) =>
        create.witnessParties.loneElement shouldBe alice
        create.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
      }
      // clear out commandId since submitter is not in the querying parties for flat transactions in the non-wildcard query
      resultPartyWildcard.loneElement.copy(commandId = "") shouldBe result.loneElement
    }
  }

  it should "filter correctly by multiple parties with different templates" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      _ <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            (alice, someTemplateId, someContractArgument),
            (bob, otherTemplateId, otherContractArgument),
            (alice, otherTemplateId, otherContractArgument),
          ),
        )
      )
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(
                relation = Map(
                  otherTemplateId -> Some(Set(bob)),
                  someTemplateId -> Some(Set(alice)),
                ),
                templateWildcardParties = Some(Set.empty),
              ),
              eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
            ),
          )
      )
      resultPartyWildcard <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(
                relation = Map(
                  otherTemplateId -> None,
                  someTemplateId -> None,
                ),
                templateWildcardParties = Some(Set.empty),
              ),
              eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
            ),
          )
      )
    } yield {
      val events = result.loneElement.events.toArray
      events should have length 2
      inside(events(0).event.created) { case Some(create) =>
        create.witnessParties.loneElement shouldBe alice
        create.templateId.value shouldBe LfEngineToApi.toApiIdentifier(someTemplateId)
      }
      inside(events(1).event.created) { case Some(create) =>
        create.witnessParties.loneElement shouldBe bob
        create.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
      }
      val eventsPartyWildcard = resultPartyWildcard.loneElement.events.toArray
      eventsPartyWildcard should have length 3
    }
  }

  it should "filter correctly by multiple parties with different template and wildcards" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, _) <- store(
        multipleCreates(
          operator = "operator",
          signatoriesAndTemplates = Seq(
            (alice, someTemplateId, someContractArgument),
            (bob, otherTemplateId, otherContractArgument),
            (alice, otherTemplateId, otherContractArgument),
          ),
        )
      )
      to <- ledgerDao.lookupLedgerEnd()
      result <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(
                Map(
                  otherTemplateId -> Some(Set(alice))
                ),
                Some(Set(bob)),
              ),
              eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
            ),
          )
      )
      resultPartyWildcard <- transactionsOf(
        ledgerDao.updateReader
          .getUpdates(
            startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
            endInclusive = to.value.lastOffset,
            internalUpdateFormat = updateFormat(
              filter = TemplatePartiesFilter(
                Map(
                  otherTemplateId -> None
                ),
                Some(Set(bob)),
              ),
              eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
            ),
          )
      )
    } yield {
      val events = result.loneElement.events.toArray
      events should have length 2
      inside(events(0).event.created) { case Some(create) =>
        create.witnessParties.loneElement shouldBe bob
        create.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
      }
      inside(events(1).event.created) { case Some(create) =>
        create.witnessParties.loneElement shouldBe alice
        create.templateId.value shouldBe LfEngineToApi.toApiIdentifier(otherTemplateId)
      }
      // clear out commandId since submitter is not in the querying parties for flat transactions in the non-wildcard query
      resultPartyWildcard.loneElement.copy(commandId = "") shouldBe result.loneElement
    }
  }

  it should "return all events in the expected order" in {
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, create) <- store(singleCreate)
      firstContractId = nonTransient(create).loneElement
      (offset, exercise) <- store(exerciseWithChild(firstContractId))
      result <- ledgerDao.updateReader
        .getUpdates(
          startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
          endInclusive = offset,
          internalUpdateFormat = updateFormat(
            filter = TemplatePartiesFilter(Map.empty, Some(exercise.actAs.toSet)),
            eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
          ),
        )
        .runWith(Sink.seq)
    } yield {
      import com.daml.ledger.api.v2.event.Event
      import com.daml.ledger.api.v2.event.Event.Event.{Archived, Created}

      val txs = extractAllTransactions(result)

      inside(txs) { case Vector(tx1, tx2) =>
        tx1.updateId shouldBe create.updateId
        tx2.updateId shouldBe exercise.updateId
        inside(tx1.events) { case Seq(Event(Created(createdEvent))) =>
          createdEvent.contractId shouldBe firstContractId.coid
        }
        inside(tx2.events) { case Seq(Event(Archived(archivedEvent)), Event(Created(_))) =>
          archivedEvent.contractId shouldBe firstContractId.coid
        }
      }
    }
  }

  it should "return the expected flat transaction for the specified offset range" in {
    for {
      (_, create1) <- store(singleCreate)
      (offset1, exercise) <- store(singleExercise(nonTransient(create1).loneElement))
      (offset2, create2) <- store(singleCreate)
      result <- ledgerDao.updateReader
        .getUpdates(
          startInclusive = offset1.increment,
          endInclusive = offset2,
          internalUpdateFormat = updateFormat(
            TemplatePartiesFilter(Map.empty, Some(exercise.actAs.toSet)),
            eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
          ),
        )
        .runWith(Sink.seq)

    } yield {
      import com.daml.ledger.api.v2.event.Event
      import com.daml.ledger.api.v2.event.Event.Event.Created

      inside(extractAllTransactions(result)) { case Vector(tx) =>
        tx.updateId shouldBe create2.updateId
        inside(tx.events) { case Seq(Event(Created(createdEvent))) =>
          createdEvent.contractId shouldBe nonTransient(create2).loneElement.coid
        }
      }
    }
  }

  it should "return error when offset range is from the future" in {
    val commands: Vector[(Offset, LedgerEntry.Transaction)] = Vector.fill(3)(singleCreate)
    val beginOffsetFromTheFuture = nextOffset()
    val endOffsetFromTheFuture = nextOffset()

    for {
      _ <- storeSync(commands)

      result <- ledgerDao.updateReader
        .getUpdates(
          startInclusive = beginOffsetFromTheFuture.increment,
          endInclusive = endOffsetFromTheFuture,
          internalUpdateFormat = updateFormat(
            TemplatePartiesFilter(Map.empty, Some(Set(alice))),
            eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
          ),
        )
        .runWith(Sink.seq)
        .failed

    } yield {
      result.getMessage should include("is beyond ledger end offset")
    }
  }

  it should "return all transactions in the specified offset range when iterating with gaps in the offsets assigned to events and a page size that ensures a page ends in such a gap" in {
    // Simulates a gap in the offsets assigned to events, as they
    // can be assigned to party allocation, package uploads and
    // configuration updates as well
    def offsetGap(): Vector[(Offset, LedgerEntry.Transaction)] = {
      nextOffset()
      Vector.empty[(Offset, LedgerEntry.Transaction)]
    }

    // the order of `nextOffset()` calls is important
    val beginOffset = nextOffset()

    val commandsWithOffsetGaps: Vector[(Offset, LedgerEntry.Transaction)] =
      Vector(singleCreate) ++ offsetGap() ++
        Vector.fill(2)(singleCreate) ++ offsetGap() ++
        Vector.fill(3)(singleCreate) ++ offsetGap() ++ offsetGap() ++
        Vector.fill(5)(singleCreate)

    val endOffset = nextOffset()

    commandsWithOffsetGaps should have length 11L

    for {
      _ <- storeSync(commandsWithOffsetGaps)
      // just for having the ledger end bumped
      _ <- ledgerDao.storePartyAdded(
        endOffset,
        None,
        Timestamp.now(),
        IndexerPartyDetails(alice, true),
      )

      // `pageSize = 2` and the offset gaps in the `commandWithOffsetGaps` above are to make sure
      // that streaming works with event pages separated by offsets that don't have events in the store
      response <- createLedgerDaoResourceOwner(
        pageSize = 2,
        eventsProcessingParallelism = 8,
        acsIdPageSize = 2,
        acsIdFetchingParallelism = 2,
        acsContractFetchingParallelism = 2,
      ).use(
        _.updateReader
          .getUpdates(
            startInclusive = beginOffset.increment,
            endInclusive = endOffset,
            internalUpdateFormat = updateFormat(
              TemplatePartiesFilter(Map.empty, Some(Set(alice))),
              eventProjectionProperties = EventProjectionProperties(verbose = true, Some(Set.empty)),
            ),
          )
          .runWith(Sink.seq)
      )(ResourceContext(executionContext))

      readTxs = extractAllTransactions(response)
    } yield {
      val readTxOffsets: Vector[Long] = readTxs.map(_.offset)
      readTxOffsets shouldBe readTxOffsets.sorted
      readTxOffsets shouldBe commandsWithOffsetGaps.map(_._1.unwrap)
    }
  }

  it should "fall back to limit-based query with consistent results" in {
    val txSeqLength = 1000
    txSeqTrial(
      trials = 10,
      txSeq = unfilteredTxSeq(length = txSeqLength),
      codePath = Gen oneOf getFlatTransactionCodePaths,
    )
  }

  private[this] def txSeqTrial(
      trials: Int,
      txSeq: Gen[Vector[Boolean]],
      codePath: Gen[FlatTransactionCodePath],
  ) = {
    import com.daml.scalautil.TraverseFMSyntax.*
    import scalaz.std.list.*
    import scalaz.std.scalaFuture.*

    val trialData = Gen
      .listOfN(trials, Gen.zip(txSeq, codePath))
      .sample getOrElse sys.error("impossible Gen failure")

    trialData
      .traverseFM { case (boolSeq, cp) =>
        for {
          from <- ledgerDao.lookupLedgerEnd()
          commands <- storeSync(boolSeq map (if (_) cp.makeMatching() else cp.makeNonMatching()))
          matchingOffsets = commands zip boolSeq collect { case ((off, _), true) =>
            off.unwrap
          }
          to <- ledgerDao.lookupLedgerEnd()
          response <- ledgerDao.updateReader
            .getUpdates(
              startInclusive = from.fold(Offset.firstOffset)(_.lastOffset.increment),
              endInclusive = to.value.lastOffset,
              internalUpdateFormat = updateFormat(
                cp.filter,
                EventProjectionProperties(verbose = true, Some(Set.empty)),
              ),
            )
            .runWith(Sink.seq)
          readOffsets = response flatMap { case (_, gtr) => Seq(gtr.getTransaction.offset) }
          readCreates = extractAllTransactions(response) flatMap (_.events)
        } yield try {
          readCreates.size should ===(boolSeq count identity)
          // we check that the offsets from the DB match the ones we had before
          // submission as a substitute for actually inspecting the events (indeed,
          // so many of the events are = as written that this would not be useful)
          readOffsets should ===(matchingOffsets)
        } catch {
          case ae: org.scalatest.exceptions.TestFailedException =>
            throw ae modifyMessage (_ map { msg =>
              msg +
                "\n  Random parameters:" +
                s"\n    actual frequency: ${boolSeq.count(identity)}/${boolSeq.size}" +
                s"\n    code path: ${cp.label}" +
                s"\n  Please copy the above 4 lines to https://github.com/digital-asset/daml/issues/7521" +
                s"\n  along with which of (JdbcLedgerDaoPostgresqlSpec, JdbcLedgerDaoH2DatabaseSpec) failed"
            })
        }
      }
      .map(_.foldLeft(succeed)((_, r) => r))
  }

  /*
  it should "get all transactions in order, 48%, onlyWildcardParties" in {
    val frequency = 48
    val txSeqLength = 1000
    val path = "onlyWildcardParties"
    txSeqTrial(
      250,
      unfilteredTxFrequencySeq(txSeqLength, frequencyPct = frequency),
      getFlatTransactionCodePaths find (_.label == path) getOrElse fail(s"$path not found"))
  }
   */

  private def storeTestFixture(): Future[(Offset, Offset, Seq[LedgerEntry.Transaction])] =
    for {
      from <- ledgerDao.lookupLedgerEnd()
      (_, t1) <- store(singleCreate)
      (_, t2) <- store(singleCreate)
      (_, t3) <- store(singleExercise(nonTransient(t2).loneElement))
      (_, t4) <- store(fullyTransient())
      to <- ledgerDao.lookupLedgerEnd()
    } yield (
      from.fold(Offset.firstOffset)(_.lastOffset.increment),
      to.value.lastOffset,
      Seq(t1, t2, t3, t4),
    )

  private def lookupIndividually(
      transactions: Seq[LedgerEntry.Transaction],
      as: Set[Party],
  ): Future[Seq[Transaction]] =
    Future
      .sequence(
        transactions.map(tx =>
          ledgerDao.updateReader
            .lookupTransactionById(tx.updateId, transactionFormatForWildcardParties(as))
        )
      )
      .map(_.flatMap(_.toList.flatMap(_.transaction.toList)))

  private def transactionsOf(
      source: Source[(Offset, GetUpdatesResponse), NotUsed]
  ): Future[Seq[Transaction]] =
    source
      .map(_._2)
      .runWith(Sink.seq)
      .map(_.map(_.getTransaction))

  // Ensure two sequences of transactions are comparable:
  // - witnesses do not have to appear in a specific order
  private def comparable(txs: Seq[Transaction]): Seq[Transaction] =
    txs.map(tx => tx.copy(events = tx.events.map(_.modifyWitnessParties(_.sorted))))

  private def extractAllTransactions(
      responses: Seq[(Offset, GetUpdatesResponse)]
  ): Vector[Transaction] =
    responses.foldLeft(Vector.empty[Transaction])((b, a) => b :+ a._2.getTransaction)

  private def createLedgerDaoResourceOwner(
      pageSize: Int,
      eventsProcessingParallelism: Int,
      acsIdPageSize: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
  ) =
    daoOwner(
      eventsPageSize = pageSize,
      eventsProcessingParallelism = eventsProcessingParallelism,
      acsIdPageSize = acsIdPageSize,
      acsIdFetchingParallelism = acsIdFetchingParallelism,
      acsContractFetchingParallelism = acsContractFetchingParallelism,
    )

  // TODO(i12297): SC much of this is repeated because we're more concerned here
  // with whether each query is tested than whether the specifics of the
  // predicate are accurate. To test the latter, the creation would have
  // to have much more detail and should be a pair of Gen[Offset => LedgerEntry.Transaction]
  // rather than a pair of simple side-effecting procedures that always
  // produce more or less the same data.  If we aren't interested in testing
  // the latter at any point, we can remove most of this.
  private val getFlatTransactionCodePaths: Seq[FlatTransactionCodePath] = {
    import JdbcLedgerDaoTransactionsSpec.FlatTransactionCodePath as Mk
    Seq(
      Mk(
        "singleWildcardParty",
        TemplatePartiesFilter(Map.empty, Some(Set(alice))),
        () => singleCreate(create(_, signatories = Set(alice))),
        () => singleCreate(create(_, signatories = Set(bob))),
        ce => (ce.signatories ++ ce.observers) contains alice,
      ),
      Mk(
        "singlePartyWithTemplates",
        TemplatePartiesFilter(Map(someTemplateId -> Some(Set(alice))), Some(Set.empty)),
        () => singleCreate(create(_, signatories = Set(alice))),
        () => singleCreate(create(_, signatories = Set(bob))),
        ce =>
          ((ce.signatories ++ ce.observers) contains alice), // TODO(i12297): && ce.templateId == Some(someTemplateId.toString)
      ),
      Mk(
        "onlyWildcardParties",
        TemplatePartiesFilter(Map.empty, Some(Set(alice, bob))),
        () => singleCreate(create(_, signatories = Set(alice))),
        () => singleCreate(create(_, signatories = Set(charlie))),
        ce => (ce.signatories ++ ce.observers) exists Set(alice, bob),
      ),
      Mk(
        "sameTemplates",
        TemplatePartiesFilter(Map(someTemplateId -> Some(Set(alice, bob))), Some(Set.empty)),
        () => singleCreate(create(_, signatories = Set(alice))),
        () => singleCreate(create(_, signatories = Set(charlie))),
        ce => (ce.signatories ++ ce.observers) exists Set(alice, bob),
      ),
      Mk(
        "mixedTemplates",
        TemplatePartiesFilter(
          Map(
            someTemplateId -> Some(Set(alice)),
            otherTemplateId -> Some(Set(bob)),
          ),
          Some(Set.empty),
        ),
        () => singleCreate(create(_, signatories = Set(alice))),
        () => singleCreate(create(_, signatories = Set(charlie))),
        ce => (ce.signatories ++ ce.observers) exists Set(alice, bob),
      ),
      Mk(
        "mixedTemplatesWithWildcardParties",
        TemplatePartiesFilter(Map(someTemplateId -> Some(Set(alice))), Some(Set(bob))),
        () => singleCreate(create(_, signatories = Set(alice))),
        () => singleCreate(create(_, signatories = Set(charlie))),
        ce => (ce.signatories ++ ce.observers) exists Set(alice, bob),
      ),
    )
  }
}

private[dao] object JdbcLedgerDaoTransactionsSpec {
  private final case class FlatTransactionCodePath(
      label: String,
      filter: TemplatePartiesFilter,
      makeMatching: () => (Offset, LedgerEntry.Transaction),
      makeNonMatching: () => (Offset, LedgerEntry.Transaction),
      // TODO(i12297): SC we don't need discriminate unless we test the event contents
      // instead of just the offsets
      discriminate: CreatedEvent => Boolean = _ => false,
  )

  private def unfilteredTxSeq(length: Int): Gen[Vector[Boolean]] =
    Gen.oneOf(1, 2, 5, 10, 20, 50, 100) flatMap { invFreq =>
      unfilteredTxFrequencySeq(length, frequencyPct = 100 / invFreq)
    }

  private def unfilteredTxFrequencySeq(length: Int, frequencyPct: Int): Gen[Vector[Boolean]] =
    Gen.containerOfN[Vector, Boolean](
      length,
      Gen.frequency((frequencyPct, true), (100 - frequencyPct, false)),
    )

  private def updateFormat(
      filter: TemplatePartiesFilter,
      eventProjectionProperties: EventProjectionProperties,
  ) = {
    val eventFormat = InternalEventFormat(
      templatePartiesFilter = filter,
      eventProjectionProperties = eventProjectionProperties,
    )
    val txFormat = Some(
      InternalTransactionFormat(internalEventFormat = eventFormat, transactionShape = AcsDelta)
    )
    InternalUpdateFormat(
      includeTransactions = txFormat,
      includeReassignments = None,
      includeTopologyEvents = None,
    )
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

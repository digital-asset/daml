// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant
import java.util.UUID

import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.participant.state.v1.RejectionReason
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success, Try}

final class PostCommitValidationSpec extends WordSpec with Matchers {

  import PostCommitValidation._
  import PostCommitValidationSpec._

  "PostCommitValidation" when {

    "run without prior history" should {

      val store =
        new PostCommitValidation.BackedBy(
          noCommittedContract(parties = List.empty),
          validatePartyAllocation = false,
        )

      "accept a create with a key" in {

        val createWithKey = genTestCreate()

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(createWithKey),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "accept a create without a key" in {

        val createWithoutKey = genTestCreate().copy(key = None)

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(createWithoutKey),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "accept an exercise of a contract created within the transaction" in {

        val createContract = genTestCreate()
        val exerciseContract = genTestExercise(createContract)

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(createContract, exerciseContract),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "accept an exercise of a contract divulged in the current transaction" in {

        val divulgedContract = genTestCreate()
        val exerciseContract = genTestExercise(divulgedContract)

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(exerciseContract),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set(divulgedContract.coid),
          )

        error shouldBe None

      }

      "reject an exercise of a contract not created in this transaction" in {

        val missingCreate = genTestCreate()
        val exerciseContract = genTestExercise(missingCreate)

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(exerciseContract),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe Some(UnknownContract)

      }

      "accept a fetch of a contract created within the transaction" in {

        val createContract = genTestCreate()

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(createContract, TxBuilder.fetch(createContract)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "accept a fetch of a contract divulged in the current transaction" in {

        val divulgedContract = genTestCreate()

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(TxBuilder.fetch(divulgedContract)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set(divulgedContract.coid),
          )

        error shouldBe None

      }

      "reject a fetch of a contract not created in this transaction" in {

        val missingCreate = genTestCreate()

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(TxBuilder.fetch(missingCreate)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe Some(UnknownContract)

      }

      "accept a successful lookup of a contract created in this transaction" in {

        val createContract = genTestCreate()

        val error =
          store.validate(
            transaction = TxBuilder
              .justCommitted(createContract, TxBuilder.lookupByKey(createContract, found = true)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "reject a successful lookup of a missing contract" in {

        val missingCreate = genTestCreate()

        val error =
          store.validate(
            transaction =
              TxBuilder.justCommitted(TxBuilder.lookupByKey(missingCreate, found = true)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe Some(
          MismatchingLookup(
            expectation = Some(missingCreate.coid),
            result = None,
          )
        )

      }

      "accept a failed lookup of a missing contract" in {

        val missingContract = genTestCreate()

        val error =
          store.validate(
            transaction =
              TxBuilder.justCommitted(TxBuilder.lookupByKey(missingContract, found = false)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe None

      }

    }

    "run with one committed contract with a key" should {

      val committedContract = genTestCreate()
      val exerciseOnCommittedContract = genTestExercise(committedContract)
      val committedContractLedgerEffectiveTime = Instant.ofEpochMilli(1000)

      val store = new PostCommitValidation.BackedBy(
        committedContracts(
          parties = List.empty,
          contractFixture = committed(
            id = committedContract.coid.coid,
            ledgerEffectiveTime = committedContractLedgerEffectiveTime,
            key = committedContract.key.map(x =>
              GlobalKey.assertBuild(committedContract.coinst.template, x.key))
          ),
        ),
        validatePartyAllocation = false,
      )

      "reject a create that would introduce a duplicate key" in {

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(committedContract),
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            divulged = Set.empty,
          )

        error shouldBe Some(DuplicateKey)

      }

      "accept an exercise on the committed contract" in {

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(exerciseOnCommittedContract),
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "reject an exercise pre-dating the committed contract" in {

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(exerciseOnCommittedContract),
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime.minusNanos(1),
            divulged = Set.empty,
          )

        error shouldBe Some(
          CausalMonotonicityViolation(
            contractLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime.minusNanos(1),
          )
        )

      }

      "accept a fetch on the committed contract" in {

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(TxBuilder.fetch(committedContract)),
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "reject a fetch pre-dating the committed contract" in {

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(TxBuilder.fetch(committedContract)),
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime.minusNanos(1),
            divulged = Set.empty,
          )

        error shouldBe Some(
          CausalMonotonicityViolation(
            contractLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime.minusNanos(1),
          )
        )

      }

      "accept a successful lookup of the committed contract" in {

        val error =
          store.validate(
            transaction =
              TxBuilder.justCommitted(TxBuilder.lookupByKey(committedContract, found = true)),
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "reject a failed lookup of the committed contract" in {

        val error =
          store.validate(
            transaction =
              TxBuilder.justCommitted(TxBuilder.lookupByKey(committedContract, found = false)),
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            divulged = Set.empty,
          )

        error shouldBe Some(
          MismatchingLookup(
            result = Some(committedContract.coid),
            expectation = None,
          )
        )

      }

    }

    "run with one divulged contract" should {

      val divulgedContract = genTestCreate()
      val exerciseOnDivulgedContract = genTestExercise(divulgedContract)

      val store = new PostCommitValidation.BackedBy(
        committedContracts(
          parties = List.empty,
          contractFixture = divulged(divulgedContract.coid.coid),
        ),
        validatePartyAllocation = false,
      )

      "accept an exercise on the divulged contract" in {

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(exerciseOnDivulgedContract),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe None

      }

      "accept a fetch on the divulged contract" in {

        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(TxBuilder.fetch(divulgedContract)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe None

      }
    }

    "run with unallocated parties" should {
      val store =
        new PostCommitValidation.BackedBy(
          noCommittedContract(List.empty),
          validatePartyAllocation = true,
        )

      "reject" in {
        val createWithKey = genTestCreate()
        val error =
          store.validate(
            transaction = TxBuilder.justCommitted(createWithKey),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
          )

        error shouldBe Some(RejectionReason.PartyNotKnownOnLedger("Some parties are unallocated"))
      }
    }
  }
}

object PostCommitValidationSpec {

  private def genTestCreate(): TxBuilder.Create =
    TxBuilder.create(
      id = s"#${UUID.randomUUID}",
      template = "foo:bar:baz",
      argument = TxBuilder.record("field" -> "value"),
      signatories = Seq("Alice"),
      observers = Seq.empty,
      key = Some("key"),
    )

  private def genTestExercise(create: TxBuilder.Create): TxBuilder.Exercise =
    TxBuilder.exercise(
      contract = create,
      choice = "SomeChoice",
      consuming = true,
      actingParties = Set("Alice"),
      argument = TxBuilder.record("field" -> "value"),
    )

  private final case class ContractFixture private (
      id: ContractId,
      ledgerEffectiveTime: Option[Instant],
      key: Option[Key],
  )

  // Very dirty hack to have a contract store fixture without persistence
  private implicit val connection: Connection = null

  private final case class ContractStoreFixture private (
      contracts: Set[ContractFixture],
      parties: List[PartyDetails])
      extends PostCommitValidationData {

    override def lookupContractKeyGlobally(key: Key)(
        implicit connection: Connection = null): Option[ContractId] =
      contracts.find(c => c.key.contains(key)).map(_.id)

    override def lookupMaximumLedgerTime(ids: Set[ContractId])(
        implicit connection: Connection = null): Try[Option[Instant]] = {
      val lookup = contracts.collect {
        case c if ids.contains(c.id) => c.ledgerEffectiveTime
      }
      if (lookup.isEmpty) Failure(notFound(ids))
      else Success(lookup.fold[Option[Instant]](None)(pickTheGreatest))
    }

    override def lookupParties(parties: Seq[Party])(
        implicit connection: Connection): List[PartyDetails] =
      this.parties.filter { party =>
        parties.contains(party.party)
      }
  }

  private def pickTheGreatest(l: Option[Instant], r: Option[Instant]): Option[Instant] =
    l.fold(r)(left => r.fold(l)(right => if (left.isAfter(right)) l else r))

  private def notFound(contractIds: Set[ContractId]): Throwable =
    new IllegalArgumentException(
      s"One or more of the following contract identifiers has been found: ${contractIds.map(_.coid).mkString(", ")}"
    )

  private def noCommittedContract(parties: List[PartyDetails]): ContractStoreFixture =
    ContractStoreFixture(Set.empty, parties)

  private def committedContracts(
      parties: List[PartyDetails],
      contractFixture: ContractFixture,
      contractFixtures: ContractFixture*,
  ): ContractStoreFixture =
    ContractStoreFixture((contractFixture +: contractFixtures).toSet, parties)

  private def committed(
      id: String,
      ledgerEffectiveTime: Instant,
      key: Option[Key],
  ): ContractFixture =
    ContractFixture(
      ContractId.assertFromString(id),
      Some(ledgerEffectiveTime),
      key,
    )

  private def divulged(id: String): ContractFixture =
    ContractFixture(
      ContractId.assertFromString(id),
      None,
      None,
    )
}

// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant
import java.util.UUID

import com.daml.platform.store.dao.events.TransactionBuilder._
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success, Try}

final class PostCommitValidationSpec extends WordSpec with Matchers {

  import PostCommitValidation._
  import PostCommitValidationSpec._

  "PostCommitValidation" when {

    "run without prior history" should {

      val emptyLedger = new PostCommitValidation.BackedBy(noCommittedContract)

      "accept a create that doesn't create a duplicate key" in {

        val createWithKey = genTestCreate()

        val validation =
          emptyLedger.validate(
            transaction = just(createWithKey),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
            submitter = Party.assertFromString("Alice"),
          )

        validation shouldBe Set.empty

      }

      "accept a create without a key" in {

        val createWithoutKey = genTestCreate().copy(key = None)

        val validation =
          emptyLedger.validate(
            transaction = just(createWithoutKey),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
            submitter = Party.assertFromString("Alice"),
          )

        validation shouldBe Set.empty

      }

      "accept an exercise of a contract created within the transaction" in {

        val createContract = genTestCreate()
        val exerciseContract = genTestExercise(createContract)

        val validation =
          emptyLedger.validate(
            transaction = just(createContract, exerciseContract),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
            submitter = Party.assertFromString("Alice"),
          )

        validation shouldBe Set.empty

      }

      "accept an exercise of a contract divulged in the current transaction" in {

        val divulgedContract = genTestCreate()
        val exerciseContract = genTestExercise(divulgedContract)

        val validation =
          emptyLedger.validate(
            transaction = just(exerciseContract),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set(divulgedContract.coid),
            submitter = Party.assertFromString("Alice"),
          )

        validation shouldBe Set.empty

      }

      "reject an exercise of a contract not created in this transaction" in {

        val missingCreate = genTestCreate()
        val exerciseContract = genTestExercise(missingCreate)

        val validation =
          emptyLedger.validate(
            transaction = just(exerciseContract),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
            submitter = Party.assertFromString("Alice"),
          )

        validation should contain theSameElementsAs Seq(UnknownContract)

      }

      "accept a successful lookup of a contract created in this transaction" in {

        val createContract = genTestCreate()

        val validation =
          emptyLedger.validate(
            transaction = just(createContract, lookupByKey(createContract, found = true)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
            submitter = Party.assertFromString("Alice"),
          )

        validation shouldBe Set.empty

      }

      "reject a successful lookup of a missing contract" in {

        val missingCreate = genTestCreate()

        val validation =
          emptyLedger.validate(
            transaction = just(lookupByKey(missingCreate, found = true)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
            submitter = Party.assertFromString("Alice"),
          )

        validation should contain allElementsOf Seq(
          MismatchingLookup(
            expectation = Some(missingCreate.coid),
            result = None,
          )
        )

      }

      "accept a failed lookup of a missing contract" in {

        val missingContract = genTestCreate()

        val validation =
          emptyLedger.validate(
            transaction = just(lookupByKey(missingContract, found = false)),
            transactionLedgerEffectiveTime = Instant.now(),
            divulged = Set.empty,
            submitter = Party.assertFromString("Alice"),
          )

        validation shouldBe Set.empty

      }

    }

  }

}

object PostCommitValidationSpec {

  private def genTestCreate(): Create =
    create(
      id = s"#${UUID.randomUUID}",
      template = "foo:bar:baz",
      argument = record("field" -> "value"),
      signatories = Seq("Alice"),
      observers = Seq.empty,
      key = Some("key"),
    )

  private def genTestExercise(create: Create): Exercise =
    exercise(
      contract = create,
      choice = "SomeChoice",
      consuming = true,
      actingParties = Set("Alice"),
      argument = record("field" -> "value"),
    )

  private final case class ContractFixture private (
      id: ContractId,
      ledgerEffectiveTime: Option[Instant],
      witnesses: Set[Party],
      key: Option[Key],
  )

  // May whoever is in charge of supernatural stuff have mercy of my sould
  private implicit val connection: Connection = null

  private final case class Fixture private (contracts: Set[ContractFixture])
      extends PostCommitValidationData {

    override def lookupContractKey(submitter: Party, key: Key)(
        implicit connection: Connection = null): Option[ContractId] =
      contracts.find(c => c.key.contains(key) && c.witnesses.contains(submitter)).map(_.id)

    override def lookupMaximumLedgerTime(ids: Set[ContractId])(
        implicit connection: Connection = null): Try[Option[Instant]] = {
      val lookup = contracts.collect {
        case c if ids.contains(c.id) => c.ledgerEffectiveTime
      }
      if (lookup.isEmpty) Failure(notFound(ids))
      else Success(lookup.fold[Option[Instant]](None)(pickTheGreater))
    }
  }

  private def pickTheGreater(l: Option[Instant], r: Option[Instant]): Option[Instant] =
    l.fold(r)(left => r.fold(l)(right => if (left.isAfter(right)) l else r))

  private def notFound(contractIds: Set[ContractId]): Throwable =
    new IllegalArgumentException(
      s"One or more of the following contract identifiers has been found: ${contractIds.map(_.coid).mkString(", ")}"
    )

  private val noCommittedContract = Fixture(Set.empty)

  private def contract(
      id: String,
      ledgerEffectiveTime: Instant,
      witness: String,
      witnesses: String*): ContractFixture =
    ContractFixture(
      ContractId.assertFromString(s"#$id"),
      Some(ledgerEffectiveTime),
      (Set(witness) ++ witnesses.toSet).map(_.asInstanceOf[Party]),
      None,
    )

  private def contract(
      id: String,
      ledgerEffectiveTime: Instant,
      key: Key,
      witness: String,
      witnesses: String*,
  ): ContractFixture =
    ContractFixture(
      ContractId.assertFromString(s"#$id"),
      Some(ledgerEffectiveTime),
      (Set(witness) ++ witnesses.toSet).map(_.asInstanceOf[Party]),
      Some(key),
    )

  private def divulgedContract(id: String, witness: String, witnesses: String*): ContractFixture =
    ContractFixture(
      ContractId.assertFromString(s"#$id"),
      None,
      (Set(witness) ++ witnesses.toSet).map(_.asInstanceOf[Party]),
      None,
    )

}

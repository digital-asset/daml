// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import com.daml.lf.value.Value.ValueText
import com.daml.platform.store.backend.{ContractStorageBackend, PartyStorageBackend}
import com.daml.platform.store.entries.PartyLedgerEntry
import com.daml.platform.store.interfaces.LedgerDaoContractsReader.KeyState
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import java.sql.Connection
import java.time.Instant
import java.util.UUID

import scala.util.{Failure, Success, Try}

final class PostCommitValidationSpec extends AnyWordSpec with Matchers {
  import PostCommitValidation._
  import PostCommitValidationSpec._

  "PostCommitValidation" when {
    "run without prior history" should {
      val fixture = noCommittedContract(parties = List.empty)
      val store = new PostCommitValidation.BackedBy(
        fixture,
        fixture,
        validatePartyAllocation = false,
      )

      "accept a create with a key" in {
        val createWithKey = genTestCreate()

        val error = store.validate(
          transaction = TxBuilder.justCommitted(createWithKey),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "accept a create without a key" in {
        val createWithoutKey = genTestCreate().copy(key = None)

        val error = store.validate(
          transaction = TxBuilder.justCommitted(createWithoutKey),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "accept an exercise of a contract created within the transaction" in {
        val createContract = genTestCreate()
        val exerciseContract = genTestExercise(createContract)

        val error = store.validate(
          transaction = TxBuilder.justCommitted(createContract, exerciseContract),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "accept an exercise of a contract divulged in the current transaction" in {
        val divulgedContract = genTestCreate()
        val exerciseContract = genTestExercise(divulgedContract)

        val error = store.validate(
          transaction = TxBuilder.justCommitted(exerciseContract),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set(divulgedContract.coid),
        )

        error shouldBe None
      }

      "reject an exercise of a contract not created in this transaction" in {
        val missingCreate = genTestCreate()
        val exerciseContract = genTestExercise(missingCreate)

        val error = store.validate(
          transaction = TxBuilder.justCommitted(exerciseContract),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.UnknownContract)
      }

      "accept a fetch of a contract created within the transaction" in {
        val createContract = genTestCreate()

        val error = store.validate(
          transaction = TxBuilder.justCommitted(createContract, txBuilder.fetch(createContract)),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "accept a fetch of a contract divulged in the current transaction" in {
        val divulgedContract = genTestCreate()

        val error = store.validate(
          transaction = TxBuilder.justCommitted(txBuilder.fetch(divulgedContract)),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set(divulgedContract.coid),
        )

        error shouldBe None
      }

      "reject a fetch of a contract not created in this transaction" in {
        val missingCreate = genTestCreate()

        val error = store.validate(
          transaction = TxBuilder.justCommitted(txBuilder.fetch(missingCreate)),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.UnknownContract)
      }

      "accept a successful lookup of a contract created in this transaction" in {
        val createContract = genTestCreate()

        val error = store.validate(
          transaction = TxBuilder
            .justCommitted(createContract, txBuilder.lookupByKey(createContract, found = true)),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "reject a successful lookup of a missing contract" in {
        val missingCreate = genTestCreate()

        val error = store.validate(
          transaction = TxBuilder.justCommitted(txBuilder.lookupByKey(missingCreate, found = true)),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe Some(
          Rejection.MismatchingLookup(expectation = Some(missingCreate.coid), result = None)
        )
      }

      "accept a failed lookup of a missing contract" in {
        val missingContract = genTestCreate()

        val error = store.validate(
          transaction =
            TxBuilder.justCommitted(txBuilder.lookupByKey(missingContract, found = false)),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "accept a create in a rollback node" in {
        val createContract = genTestCreate()
        val builder = TxBuilder()
        val rollback = builder.add(builder.rollback())
        builder.add(createContract, rollback)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "accept a create after a rolled back create with the same key" in {
        val createContract = genTestCreate()
        val builder = TxBuilder()
        val rollback = builder.add(builder.rollback())
        builder.add(createContract, rollback)
        builder.add(createContract)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "reject a create in a rollback after a create with the same key" in {
        val createContract = genTestCreate()
        val builder = TxBuilder()
        builder.add(createContract)
        val rollback = builder.add(builder.rollback())
        builder.add(createContract, rollback)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.DuplicateKey)
      }

      "reject a create after a rolled back archive of a contract with the same key" in {
        val createContract = genTestCreate()
        val builder = TxBuilder()
        builder.add(createContract)
        val rollback = builder.add(builder.rollback())
        builder.add(genTestExercise(createContract), rollback)
        builder.add(createContract)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.DuplicateKey)
      }

      "accept a failed lookup in a rollback" in {
        val createContract = genTestCreate()
        val builder = TxBuilder()
        val rollback = builder.add(builder.rollback())
        builder.add(builder.lookupByKey(createContract, found = false), rollback)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }
    }

    "run with one committed contract with a key" should {
      val committedContract = genTestCreate()
      val exerciseOnCommittedContract = genTestExercise(committedContract)
      val committedContractLedgerEffectiveTime =
        Timestamp.assertFromInstant(Instant.ofEpochMilli(1000))

      val fixture = committedContracts(
        parties = List.empty,
        contractFixture = committed(
          id = committedContract.coid.coid,
          ledgerEffectiveTime = committedContractLedgerEffectiveTime,
          key = committedContract.key.map(x =>
            GlobalKey.assertBuild(committedContract.templateId, x.key)
          ),
        ),
      )
      val store = new PostCommitValidation.BackedBy(
        fixture,
        fixture,
        validatePartyAllocation = false,
      )

      "reject a create that would introduce a duplicate key" in {
        val error = store.validate(
          transaction = TxBuilder.justCommitted(committedContract),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.DuplicateKey)
      }

      "accept an exercise on the committed contract" in {
        val error = store.validate(
          transaction = TxBuilder.justCommitted(exerciseOnCommittedContract),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "reject an exercise pre-dating the committed contract" in {
        val error = store.validate(
          transaction = TxBuilder.justCommitted(exerciseOnCommittedContract),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime.addMicros(-1),
          divulged = Set.empty,
        )

        error shouldBe Some(
          Rejection.CausalMonotonicityViolation(
            contractLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime.addMicros(-1),
          )
        )
      }

      "accept a fetch on the committed contract" in {
        val error = store.validate(
          transaction = TxBuilder.justCommitted(txBuilder.fetch(committedContract)),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "reject a fetch pre-dating the committed contract" in {
        val error = store.validate(
          transaction = TxBuilder.justCommitted(txBuilder.fetch(committedContract)),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime.addMicros(-1),
          divulged = Set.empty,
        )

        error shouldBe Some(
          Rejection.CausalMonotonicityViolation(
            contractLedgerEffectiveTime = committedContractLedgerEffectiveTime,
            transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime.addMicros(-1),
          )
        )
      }

      "accept a successful lookup of the committed contract" in {
        val error = store.validate(
          transaction =
            TxBuilder.justCommitted(txBuilder.lookupByKey(committedContract, found = true)),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "reject a failed lookup of the committed contract" in {
        val error = store.validate(
          transaction =
            TxBuilder.justCommitted(txBuilder.lookupByKey(committedContract, found = false)),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe Some(
          Rejection.MismatchingLookup(result = Some(committedContract.coid), expectation = None)
        )
      }

      "reject a create in a rollback" in {
        val builder = TxBuilder()
        val rollback = builder.add(builder.rollback())
        builder.add(committedContract, rollback)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.DuplicateKey)
      }

      "reject a failed lookup in a rollback" in {
        val builder = TxBuilder()
        val rollback = builder.add(builder.rollback())
        builder.add(builder.lookupByKey(committedContract, found = false), rollback)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe Some(
          Rejection.MismatchingLookup(
            result = Some(committedContract.coid),
            expectation = None,
          )
        )
      }

      "accept a successful lookup in a rollback" in {
        val builder = TxBuilder()
        val rollback = builder.add(builder.rollback())
        builder.add(builder.lookupByKey(committedContract, found = true), rollback)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "reject a create after a rolled back archive" in {
        val builder = TxBuilder()
        val rollback = builder.add(builder.rollback())
        builder.add(genTestExercise(committedContract), rollback)
        builder.add(committedContract)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = committedContractLedgerEffectiveTime,
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.DuplicateKey)
      }
    }

    "run with one divulged contract" should {
      val divulgedContract = genTestCreate()
      val exerciseOnDivulgedContract = genTestExercise(divulgedContract)

      val fixture = committedContracts(
        parties = List.empty,
        contractFixture = divulged(divulgedContract.coid.coid),
      )
      val store = new PostCommitValidation.BackedBy(
        fixture,
        fixture,
        validatePartyAllocation = false,
      )

      "accept an exercise on the divulged contract" in {
        val error = store.validate(
          transaction = TxBuilder.justCommitted(exerciseOnDivulgedContract),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }

      "accept a fetch on the divulged contract" in {
        val error = store.validate(
          transaction = TxBuilder.justCommitted(txBuilder.fetch(divulgedContract)),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe None
      }
    }

    "run with unallocated parties" should {
      val store = new PostCommitValidation.BackedBy(
        noCommittedContract(List.empty),
        noCommittedContract(List.empty),
        validatePartyAllocation = true,
      )

      "reject" in {
        val createWithKey = genTestCreate()
        val error = store.validate(
          transaction = TxBuilder.justCommitted(createWithKey),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.UnallocatedParties)
      }

      "reject if party is used in rollback" in {
        val createWithKey = genTestCreate()
        val builder = TxBuilder()
        val rollback = builder.add(builder.rollback())
        builder.add(createWithKey, rollback)

        val error = store.validate(
          transaction = builder.buildCommitted(),
          transactionLedgerEffectiveTime = Timestamp.now(),
          divulged = Set.empty,
        )

        error shouldBe Some(Rejection.UnallocatedParties)
      }
    }
  }
}

object PostCommitValidationSpec {

  import TxBuilder.Implicits._

  // Very dirty hack to have a contract store fixture without persistence
  private implicit val connection: Connection = null

  private val txBuilder = TxBuilder()

  private def genTestCreate(): Create =
    txBuilder.create(
      id = s"#${UUID.randomUUID}",
      templateId = "bar:baz",
      argument = TxBuilder.record("field" -> "value"),
      signatories = Set("Alice"),
      observers = Set.empty,
      key = Some(ValueText("key")),
    )

  private def genTestExercise(create: Create): Exercise =
    txBuilder.exercise(
      contract = create,
      choice = "SomeChoice",
      consuming = true,
      actingParties = Set("Alice"),
      argument = TxBuilder.record("field" -> "value"),
    )

  private final case class ContractFixture private (
      id: ContractId,
      ledgerEffectiveTime: Option[Timestamp],
      key: Option[Key],
  )

  private final case class ContractStoreFixture private (
      contracts: Set[ContractFixture],
      parties: List[PartyDetails],
  ) extends PartyStorageBackend
      with ContractStorageBackend {
    override def contractKeyGlobally(key: Key)(connection: Connection): Option[ContractId] =
      contracts.find(c => c.key.contains(key)).map(_.id)
    override def maximumLedgerTime(
        ids: Set[ContractId]
    )(connection: Connection): Try[Option[Timestamp]] = {
      val lookup = contracts.collect {
        case c if ids.contains(c.id) => c.ledgerEffectiveTime
      }
      if (lookup.isEmpty) Failure(notFound(ids))
      else Success(lookup.fold[Option[Timestamp]](None)(pickTheGreatest))
    }
    override def keyState(key: Key, validAt: Long)(connection: Connection): KeyState =
      notImplemented()
    override def contractState(contractId: ContractId, before: Long)(
        connection: Connection
    ): Option[ContractStorageBackend.RawContractState] = notImplemented()
    override def activeContractWithArgument(readers: Set[Ref.Party], contractId: ContractId)(
        connection: Connection
    ): Option[ContractStorageBackend.RawContract] = notImplemented()
    override def activeContractWithoutArgument(readers: Set[Ref.Party], contractId: ContractId)(
        connection: Connection
    ): Option[String] = notImplemented()
    override def contractKey(readers: Set[Ref.Party], key: Key)(
        connection: Connection
    ): Option[ContractId] = notImplemented()
    override def contractStateEvents(startExclusive: Long, endInclusive: Long)(
        connection: Connection
    ): Vector[ContractStorageBackend.RawContractStateEvent] = notImplemented()

    override def partyEntries(
        startExclusive: Offset,
        endInclusive: Offset,
        pageSize: Int,
        queryOffset: Long,
    )(connection: Connection): Vector[(Offset, PartyLedgerEntry)] = notImplemented()
    override def parties(parties: Seq[Party])(connection: Connection): List[PartyDetails] =
      this.parties.filter { party =>
        parties.contains(party.party)
      }
    override def knownParties(connection: Connection): List[PartyDetails] = notImplemented()

    // PostCommitValidation only uses a small subset of (PartyStorageBackend with ContractStorageBackend)
    private def notImplemented() =
      throw new RuntimeException(
        "This method is not implemented because it's not used by PostCommitValidation"
      )
  }

  private def pickTheGreatest(l: Option[Timestamp], r: Option[Timestamp]): Option[Timestamp] =
    l.fold(r)(left => r.fold(l)(right => if (left > right) l else r))

  private def notFound(contractIds: Set[ContractId]): Throwable =
    new IllegalArgumentException(
      s"One or more of the following contract identifiers has not been found: ${contractIds.map(_.coid).mkString(", ")}"
    )

  private def noCommittedContract(parties: List[PartyDetails]): ContractStoreFixture =
    ContractStoreFixture(
      contracts = Set.empty,
      parties = parties,
    )

  private def committedContracts(
      parties: List[PartyDetails],
      contractFixture: ContractFixture,
      contractFixtures: ContractFixture*
  ): ContractStoreFixture =
    ContractStoreFixture(
      contracts = (contractFixture +: contractFixtures).toSet,
      parties = parties,
    )

  private def committed(
      id: String,
      ledgerEffectiveTime: Timestamp,
      key: Option[Key],
  ): ContractFixture =
    ContractFixture(
      id = ContractId.assertFromString(id),
      ledgerEffectiveTime = Some(ledgerEffectiveTime),
      key = key,
    )

  private def divulged(id: String): ContractFixture =
    ContractFixture(
      id = ContractId.assertFromString(id),
      ledgerEffectiveTime = None,
      key = None,
    )
}

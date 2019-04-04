// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Identifier, SimpleString}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractInst,
  ValueText,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueCoder.{DecodeError, EncodeError}
import com.digitalasset.daml.lf.value.ValueVersions
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.backend.api.v1.RejectionReason
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.EventId
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{Contract, PostgresLedgerDao}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  TransactionSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.PropertyChecks
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

//TODO: use scalacheck when we have generators available for contracts and transactions
class PostgresDaoSpec
    extends AsyncWordSpec
    with Matchers
    with PostgresAroundAll
    with PropertyChecks {

  private val mockTransactionSerialiser = new TransactionSerializer {
    override def serialiseTransaction(
        transaction: GenTransaction[
          EventId,
          AbsoluteContractId,
          VersionedValue[AbsoluteContractId]]): Either[EncodeError, Array[Byte]] =
      Right(Array.empty)

    override def deserializeTransaction(blob: Array[Byte]): Either[
      DecodeError,
      GenTransaction[EventId, AbsoluteContractId, VersionedValue[AbsoluteContractId]]] =
      Right(null)
  }

  private lazy val dbDispatcher = DbDispatcher(postgresFixture.jdbcUrl, testUser, 4)
  private lazy val ledgerDao =
    PostgresLedgerDao(dbDispatcher, ContractSerializer, mockTransactionSerialiser)

  private val nextOffset: () => Long = {
    val counter = new AtomicLong(0)
    () =>
      counter.getAndIncrement()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Await.result(ledgerDao.storeInitialLedgerEnd(nextOffset()), Duration.Inf)
  }

  "Postgres Ledger DAO" should {
    "be able to persist and load contracts" in {
      val offset = nextOffset()

      val transaction = LedgerEntry.Transaction(
        "commandId1",
        "trId1",
        "appID1",
        "Alice",
        "workflowId",
        Instant.now,
        Instant.now,
        null,
        Map("event1" -> Set("Alice", "Bob"), "event2" -> Set("Alice", "In", "Chains"))
      )

      val absCid = AbsoluteContractId("cId")
      val contractInstance = ContractInst(
        Identifier(
          Ref.PackageId.assertFromString("packageId"),
          Ref.QualifiedName(
            Ref.ModuleName.assertFromString("moduleName"),
            Ref.DottedName.assertFromString("name"))),
        VersionedValue(ValueVersions.acceptedVersions.head, ValueText("some text")),
        "agreement"
      )
      val contract = Contract(
        absCid,
        Instant.EPOCH,
        "trId1",
        "workflowId",
        Set(SimpleString.assertFromString("Alice"), SimpleString.assertFromString("Bob")),
        contractInstance)

      for {
        _ <- ledgerDao.storeLedgerEntry(offset, transaction)
        result1 <- ledgerDao.lookupActiveContract(absCid)
        _ <- ledgerDao.storeContract(contract)
        result2 <- ledgerDao.lookupActiveContract(absCid)
      } yield {
        result1 shouldEqual None
        result2 shouldEqual Some(contract)
      }
    }

    "be able to persist and load a checkpoint" in {
      val checkpoint = LedgerEntry.Checkpoint(Instant.now)
      val offset = nextOffset()

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(offset, checkpoint)
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual (Some(checkpoint))
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

    val rejectionReasonGen: Gen[RejectionReason] = for {
      const <- Gen.oneOf[String => RejectionReason](
        Seq[String => RejectionReason](
          RejectionReason.Inconsistent.apply(_),
          RejectionReason.OutOfQuota.apply(_),
          RejectionReason.TimedOut.apply(_),
          RejectionReason.Disputed.apply(_),
          RejectionReason.DuplicateCommandId.apply(_)
        ))
      desc <- Arbitrary.arbitrary[String].filter(_.nonEmpty)
    } yield const(desc)

    "be able to persist and load a rejection" in {
      forAll(rejectionReasonGen) { rejectionReason =>
        val offset = nextOffset()
        val rejection = LedgerEntry.Rejection(
          Instant.now,
          s"commandId-$offset",
          s"applicationId-$offset",
          "party",
          rejectionReason)

        @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
        implicit val ec = DirectExecutionContext

        val resultF = for {
          startingOffset <- ledgerDao.lookupLedgerEnd()
          _ <- ledgerDao.storeLedgerEntry(offset, rejection)
          entry <- ledgerDao.lookupLedgerEntry(offset)
          endingOffset <- ledgerDao.lookupLedgerEnd()
        } yield {
          entry shouldEqual Some(rejection)
          endingOffset shouldEqual (startingOffset + 1)
        }

        Await.result(resultF, Duration.Inf)
      }
    }

    "be able to persist and load a transaction" in {
      val offset = nextOffset()

      val transaction = LedgerEntry.Transaction(
        "commandId2",
        "trId2",
        "appID2",
        "Alice",
        "workflowId",
        Instant.now,
        Instant.now,
        null,
        Map("event1" -> Set("Alice", "Bob"), "event2" -> Set("Alice", "In", "Chains"))
      )

      for {
        startingOffset <- ledgerDao.lookupLedgerEnd()
        _ <- ledgerDao.storeLedgerEntry(offset, transaction)
        entry <- ledgerDao.lookupLedgerEntry(offset)
        endingOffset <- ledgerDao.lookupLedgerEnd()
      } yield {
        entry shouldEqual Some(transaction)
        endingOffset shouldEqual (startingOffset + 1)
      }
    }

  }

}

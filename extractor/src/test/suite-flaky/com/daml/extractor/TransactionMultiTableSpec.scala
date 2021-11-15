// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File
import java.sql.Timestamp
import java.time.Instant

import cats.implicits._
import com.daml.bazeltools.BazelRunfiles._
import com.daml.extractor.services.{CustomMatchers, ExtractorFixtureAroundAll}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import doobie.implicits._
import io.circe.syntax._
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList

import scala.concurrent.duration._

class TransactionMultiTableSpec
    extends AnyFlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Inspectors
    with Matchers
    with CustomMatchers {

  import services.Types._

  override protected def darFile = new File(rlocation("extractor/test.dar"))

  override protected val initScript = Some("TransactionExample:example")

  override protected def parties: NonEmptyList[String] = NonEmptyList("Example1")

  override protected def outputFormat: String = "multi-table"

  "Transactions" should "be extracted" in {
    getTransactions should have length 3
  }

  it should "be valid transactions" in {
    forAll(getTransactions) { transaction =>
      inside(transaction) {
        case TransactionResult(
              transaction_id,
              seq,
              _, // Daml Script leaves the workflow identifier empty
              effective_at,
              extracted_at,
              ledger_offset,
            ) =>
          transaction_id should not be empty
          seq should be >= 1
          effective_at should be(new Timestamp(0L))
          extracted_at should beWithin(30.seconds)(Timestamp.from(Instant.now()))
          ledger_offset should not be empty
      }
    }
  }

  it should "be transactions with different ids" in {
    val transactions = getTransactions

    transactions.map(_.transaction_id).toSet should have size 3
    transactions.map(_.seq).toSet should have size 3
    transactions.map(_.ledger_offset).toSet should have size 3
  }

  "Exercises" should "be extracted" in {
    getExercises should have length 2
  }

  "All the data" should "represent what went down in the script" in {
    // `transaction1` created `contract1`, then
    // `transaction2` created `exercise`, which archived `contract1` and resulted `contract2`

    val List(transaction1, transaction2, transaction3) = getTransactions.sortBy(_.seq)
    val List(exercise1, exercise2) = getExercises
    val List(
      (archived_by_event_id_offer1, transaction_id_offer1, archived_by_transaction_id_offer1),
      (archived_by_event_id_offer2, transaction_id_offer2, archived_by_transaction_id_offer2),
    ) =
      getResultList[(Option[String], String, Option[String])](
        sql"SELECT _archived_by_event_id, _transaction_id, _archived_by_transaction_id FROM template.transactionexample_rightofuseoffer"
      )
    val List(
      (
        event_id_accept,
        archived_by_event_id_accept,
        transaction_id_accept,
        archived_by_transaction_id_accept,
      )
    ) =
      getResultList[(String, Option[String], String, Option[String])](
        sql"SELECT _event_id, _archived_by_event_id, _transaction_id, _archived_by_transaction_id FROM template.transactionexample_rightofuseagreement"
      )

    // `transaction1` created `contract1` (first offer), then
    transaction_id_offer1 shouldEqual transaction1.transaction_id

    // `transaction2` created `exercise1`
    exercise1.transaction_id shouldEqual transaction2.transaction_id

    // `exercise1` archived `contract1` (first offer)
    archived_by_transaction_id_offer1 shouldEqual Some(transaction2.transaction_id)
    archived_by_event_id_offer1 shouldEqual Some(exercise1.event_id)

    // ... while it resulted in `contract2` (first accept)
    exercise1.child_event_ids.asArray.toList.toVector.flatten should contain(event_id_accept.asJson)
    transaction_id_accept shouldEqual transaction2.transaction_id
    // which is not archived
    archived_by_transaction_id_accept shouldEqual None
    archived_by_event_id_accept shouldEqual None

    // `transaction3` (second containing an offer) created `contract3` (second offer); then
    transaction_id_offer2 shouldEqual transaction3.transaction_id

    // `transaction3` created `exercise2`
    exercise2.transaction_id shouldEqual transaction3.transaction_id

    // `exercise2` archived `contract3` (second offer)
    archived_by_transaction_id_offer2 shouldEqual Some(transaction3.transaction_id)
    archived_by_event_id_offer2 shouldEqual Some(exercise2.event_id)
  }

}

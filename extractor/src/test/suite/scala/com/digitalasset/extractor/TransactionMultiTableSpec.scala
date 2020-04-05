// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TransactionMultiTableSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Inspectors
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File(rlocation("extractor/TransactionExample.dar"))

  override def scenario: Option[String] = Some("TransactionExample:example")

  override protected def outputFormat: String = "multi-table"

  "Transactions" should "be extracted" in {
    getTransactions should have length 2
  }

  it should "be valid transactions" in {
    forAll(getTransactions) { transaction =>
      inside(transaction) {
        case TransactionResult(
            transaction_id,
            seq,
            workflow_id,
            effective_at,
            extracted_at,
            ledger_offset
            ) =>
          transaction_id should not be empty
          seq should be >= 1
          workflow_id should not be empty
          effective_at should be(new Timestamp(0L))
          extracted_at should beWithin(30.seconds)(Timestamp.from(Instant.now()))
          ledger_offset should not be empty
      }
    }
  }

  it should "be transactions with different ids" in {
    val transactions = getTransactions

    transactions.map(_.transaction_id).toSet should have size 2
    transactions.map(_.workflow_id).toSet should have size 2
    transactions.map(_.seq).toSet should have size 2
    transactions.map(_.ledger_offset).toSet should have size 2
  }

  "Exercises" should "be extracted" in {
    getExercises should have length 1
  }

  "All the data" should "represent what went down in the scenario" in {
    // `transaction1` created `contract1`, then
    // `transaction2` created `exercise`, which archived `contract1` and resulted `contract2`

    val List(transaction1, transaction2) = getTransactions.sortBy(_.seq)
    val List(exercise) = getExercises
    val List((archived_by_event_id1, transaction_id1, archived_by_transaction_id1)) =
      getResultList[(Option[String], String, Option[String])](
        sql"SELECT _archived_by_event_id, _transaction_id, _archived_by_transaction_id FROM template.transactionexample_rightofuseoffer")
    val List((event_id2, archived_by_event_id2, transaction_id2, archived_by_transaction_id2)) =
      getResultList[(String, Option[String], String, Option[String])](
        sql"SELECT _event_id, _archived_by_event_id, _transaction_id, _archived_by_transaction_id FROM template.transactionexample_rightofuseagreement")

    // `transaction1` created `contract1`, then
    transaction_id1 shouldEqual transaction1.transaction_id

    // `transaction2` created `exercise`
    exercise.transaction_id shouldEqual transaction2.transaction_id

    // `exercised` archived `contract1`
    archived_by_transaction_id1 shouldEqual Some(transaction2.transaction_id)
    archived_by_event_id1 shouldEqual Some(exercise.event_id)

    // ... while it resulted in `contract2`
    exercise.child_event_ids.asArray.toList.toVector.flatten should contain(event_id2.asJson)
    transaction_id2 shouldEqual transaction2.transaction_id
    // which is not archived
    archived_by_transaction_id2 shouldEqual None
    archived_by_event_id2 shouldEqual None
  }

}

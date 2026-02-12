// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.postgresql.util.{PSQLException, PSQLState}
import org.scalatest.wordspec.AnyWordSpec
import slick.dbio
import slick.dbio.{DBIOAction, Effect, NoStream}

class DBIOUtilTest extends AnyWordSpec with BaseTest with PostgresTest with DbTest {

  override protected def cleanDb(
      storage: DbStorage
  )(implicit tc: TraceContext): FutureUnlessShutdown[?] = FutureUnlessShutdown.unit

  "batchedSequentialTraverse" should {
    // The test builds a DBIOAction AST that fails on purpose, so that we can test the retry behavior
    "properly rebuild the action AST upon retry" in {
      val serializableException =
        new PSQLException("forced serializable error", PSQLState.SERIALIZATION_FAILURE)

      val actionsSeq = Seq[DBIOAction[Int, NoStream, Effect]](
        dbio.DBIO.successful(1),
        dbio.DBIO.successful(2),
        dbio.DBIO.failed(serializableException),
      )

      // Using plain MonadUtil.foldLeftM, the AST will be built by holding on to a reference of the iterator.
      // Upon retry,  this iterator will be at the "end" and the reconstruction of the AST will not actually take place.
      val dbioActionWrongBehavior =
        MonadUtil
          .foldLeftM[DBIOAction[*, NoStream, Effect], Int, DBIOAction[Int, NoStream, Effect]](
            0,
            actionsSeq,
          ) { case (_, action) => action }
      val unexpectedSuccessF =
        storage.update(
          dbioActionWrongBehavior,
          operationName = "expected-failure",
          maxRetries = 2,
        )
      // The reconstructed action on retry is effectively just
      unexpectedSuccessF.futureValueUS shouldBe 1

      val retrySafeDbioAction =
        DBIOUtil.batchedSequentialTraverse(chunkSize = PositiveInt.one)(actionsSeq) {
          case (_idx, Seq(action)) =>
            action
          case _ => sys.error("should not happen")
        }
      val expectedFailureF =
        storage.update(retrySafeDbioAction, operationName = "expected-failure", maxRetries = 2)
      expectedFailureF.failed.futureValueUS shouldBe serializableException
    }
  }
}

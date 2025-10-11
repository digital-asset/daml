// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.PendingOperation.PendingOperationTriggerType
import com.digitalasset.canton.store.PendingOperationStoreTest.TestPendingOperationMessage
import com.digitalasset.canton.store.{
  PendingOperation,
  PendingOperationStore,
  PendingOperationStoreTest,
}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import org.h2.jdbc.JdbcSQLDataException
import org.postgresql.util.PSQLException
import org.scalatest.BeforeAndAfterAll
import slick.jdbc.SetParameter

import scala.annotation.unused
import scala.concurrent.Future

sealed trait DbPendingOperationsStoreTest
    extends PendingOperationStoreTest[TestPendingOperationMessage]
    with BeforeAndAfterAll {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update_(
      sqlu"truncate table common_pending_operations restart identity",
      functionFullName,
    )
  }

  override protected def insertCorruptedData(
      op: PendingOperation[TestPendingOperationMessage],
      store: Option[PendingOperationStore[TestPendingOperationMessage]] = None,
      corruptOperationBytes: Option[ByteString] = None,
  ): Future[Unit] = {
    import DbStorage.Implicits.setParameterByteString
    import storage.api.*
    @unused
    implicit val setParameter: SetParameter[TestPendingOperationMessage] =
      (v: TestPendingOperationMessage, pp) => pp >> v.toByteString
    @unused
    implicit val setOperationTriggerType: SetParameter[PendingOperationTriggerType] =
      DbPendingOperationsStore.setOperationTriggerType(storage)

    val operationBytes = corruptOperationBytes.getOrElse(ByteString.empty())

    val upsertCorruptAction = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
        insert into common_pending_operations
          (operation_trigger, operation_name, operation_key, operation, synchronizer_id)
        values
          (${op.trigger}, ${op.name.unwrap}, ${op.key}, $operationBytes, ${op.synchronizerId})
        on conflict (synchronizer_id, operation_key, operation_name) do update
          set operation = $operationBytes
      """
      case _: DbStorage.Profile.H2 =>
        sqlu"""
        merge into common_pending_operations
          (operation_trigger, operation_name, operation_key, operation, synchronizer_id)
        key (synchronizer_id, operation_key, operation_name)
        values
          (${op.trigger}, ${op.name.unwrap}, ${op.key}, $operationBytes, ${op.synchronizerId})
      """
    }
    storage.update_(upsertCorruptAction, functionFullName).failOnShutdown

  }

  "DbPendingOperationsStore" should {
    behave like pendingOperationsStore(() =>
      new DbPendingOperationsStore(storage, timeouts, loggerFactory, TestPendingOperationMessage)
    )

    "fail on write when inserting an invalid trigger type" in {
      import DbStorage.Implicits.setParameterByteString
      import storage.api.*

      val insertInvalidTriggerType =
        sqlu"""
          insert into common_pending_operations(operation_trigger, operation_name, operation_key, operation, synchronizer_id)
          values ('invalid_trigger_type', 'valid-name', 'valid-key', ${ByteString.empty}, ${DefaultTestIdentities.synchronizerId})
        """

      val resultF: Future[Unit] = storage
        .update_(
          insertInvalidTriggerType,
          "fail on write when inserting an invalid trigger type",
        )
        .failOnShutdown

      storage.profile match {
        case _: DbStorage.Profile.Postgres =>
          recoverToSucceededIf[PSQLException](resultF)
        case _: DbStorage.Profile.H2 =>
          recoverToSucceededIf[JdbcSQLDataException](resultF)
      }
    }
  }
}

class PendingOperationsStoreTestH2 extends DbPendingOperationsStoreTest with H2Test

class PendingOperationsStorePostgres extends DbPendingOperationsStoreTest with PostgresTest

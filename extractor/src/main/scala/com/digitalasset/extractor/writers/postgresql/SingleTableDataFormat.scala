// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers.postgresql

import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.extractor.ledger.types.{CreatedEvent, ExercisedEvent, TransactionTree}
import com.digitalasset.extractor.writers.Writer
import com.digitalasset.extractor.writers.postgresql.DataFormat.TemplateInfo
import com.digitalasset.extractor.writers.postgresql.DataFormatState.SingleTableState

import cats.implicits._
import doobie.implicits._
import doobie.free.connection
import doobie.free.connection.ConnectionIO

import scalaz._, Scalaz._

class SingleTableDataFormat extends DataFormat[SingleTableState.type] {
  import Queries._
  import Queries.SingleTable._

  def init(): ConnectionIO[Unit] = {
    dropContractsTable.update.run *>
      createContractsTable.update.run *>
      // the order of the index is important as people will seach by either template or both
      createIndex("contract", NonEmptyList("template", "package_id")).update.run.void
  }

  def handleTemplate(
      state: SingleTableState.type,
      packageStore: PackageStore,
      template: TemplateInfo
  ): (DataFormatState.SingleTableState.type, ConnectionIO[Unit]) = {
    // whatevs, we have a single table
    (state, connection.pure(()))
  }

  def handlePackageId(
      state: DataFormatState.SingleTableState.type,
      packageId: String
  ): (DataFormatState.SingleTableState.type, ConnectionIO[Unit]) = {
    // whatevs, we have a single table
    (state, connection.pure(()))
  }

  def handleExercisedEvent(
      state: DataFormatState.SingleTableState.type,
      transaction: TransactionTree,
      event: ExercisedEvent
  ): Writer.RefreshPackages \/ ConnectionIO[Unit] = {
    val query =
      setContractArchived(event.contractCreatingEventId, transaction.transactionId, event.eventId)

    query.update.run.void.right
  }

  def handleCreatedEvent(
      state: DataFormatState.SingleTableState.type,
      transaction: TransactionTree,
      event: CreatedEvent
  ): Writer.RefreshPackages \/ ConnectionIO[Unit] = {
    insertContract(
      event,
      transaction.transactionId,
      transaction.rootEventIds.contains(event.eventId)).update.run.void.right
  }
}

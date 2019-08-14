// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers.postgresql

import com.digitalasset.daml.lf.iface
import com.digitalasset.extractor.ledger.types._
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.extractor.writers.Writer.RefreshPackages

import doobie.free.connection.ConnectionIO

import scalaz._

final case class TableName(withSchema: String, withOutSchema: String)

sealed abstract class DataFormatState extends Product with Serializable
object DataFormatState {
  case object SingleTableState extends DataFormatState
  final case class MultiTableState(
      templateToTable: Map[Identifier, TableName],
      packageIdToNameSpace: Map[String, String]
  ) extends DataFormatState
}

import DataFormat._

/**
  * Abstraction over different data formats
  *
  * Currently the two implementations are used directly,
  * so it could work without this trait, but it might come handy in the future
  */
trait DataFormat[S <: DataFormatState] {
  def init(): ConnectionIO[Unit]
  def handleTemplate(
      state: S,
      packageStore: PackageStore,
      template: TemplateInfo): (S, ConnectionIO[Unit])
  def handlePackageId(state: S, packageId: String): (S, ConnectionIO[Unit])
  def handleExercisedEvent(
      state: S,
      transaction: TransactionTree,
      event: ExercisedEvent
  ): RefreshPackages \/ ConnectionIO[Unit]
  def handleCreatedEvent(
      state: S,
      transaction: TransactionTree,
      event: CreatedEvent
  ): RefreshPackages \/ ConnectionIO[Unit]
}

object DataFormat {
  type TemplateInfo = (Identifier, iface.Record.FWT)
}

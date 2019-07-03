// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers

import com.digitalasset.extractor.config.ExtractorConfig
import com.digitalasset.ledger.service.LedgerReader.PackageStore
import com.digitalasset.extractor.ledger.types.{Identifier, TransactionTree}
import com.digitalasset.extractor.targets._

import scala.concurrent.Future
import scalaz._

trait Writer {
  import Writer._
  def init(): Future[Unit]
  def handlePackages(packageStore: PackageStore): Future[Unit]
  def handleTransaction(transaction: TransactionTree): Future[RefreshPackages \/ Unit]
  def getLastOffset: Future[Option[String]]
}

object Writer {
  final case class RefreshPackages(missing: Identifier)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def apply[T <: Target](config: ExtractorConfig, target: T, ledgerId: String): Writer =
    target match {
      case TextPrintTarget => new SimpleTextWriter(println)
      case t: PrettyPrintTarget => new PrettyPrintWriter(t)
      case t: PostgreSQLTarget => new PostgreSQLWriter(config, t, ledgerId)
    }
}

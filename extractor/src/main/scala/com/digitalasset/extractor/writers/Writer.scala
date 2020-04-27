// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.writers

import com.daml.extractor.config.ExtractorConfig
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.extractor.ledger.types.{Identifier, TransactionTree}
import com.daml.extractor.targets._

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
  def apply(config: ExtractorConfig, target: Target, ledgerId: String): Writer =
    target match {
      case TextPrintTarget => new SimpleTextWriter(println)
      case t: PrettyPrintTarget => new PrettyPrintWriter(t)
      case t: PostgreSQLTarget => new PostgreSQLWriter(config, t, ledgerId)
    }
}

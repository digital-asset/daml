// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.writers

import com.digitalasset.daml.lf.iface.reader.Interface
import com.digitalasset.extractor.ledger.LedgerReader.PackageStore
import com.digitalasset.extractor.ledger.types.{Event, TransactionTree}
import com.digitalasset.extractor.writers.Writer.RefreshPackages

import scala.concurrent.Future
import scalaz._
import Scalaz._

trait PrinterFunctionWriter { self: Writer =>

  def printer: Any => Unit

  def handlePackage(id: String, interface: Interface): Unit

  def printEvent(event: Event): Unit

  def init(): Future[Unit] = {
    printer("==============")
    printer("DAML Extractor")
    printer("==============")

    Future.successful(())
  }

  def handlePackages(packageStore: PackageStore): Future[Unit] = {
    printer("====================")
    printer("Handling packages...")
    printer("====================")
    packageStore.packages.foreach((handlePackage _).tupled)

    Future.successful(())
  }

  def handleTransaction(transaction: TransactionTree): Future[RefreshPackages \/ Unit] = {
    printer(s"Handling transaction #${transaction.transactionId}...")
    printer(s"Events:")
    transaction.events.values.foreach(printEvent)

    Future.successful(().right)
  }

  def getLastOffset: Future[Option[String]] = Future.successful(None)
}

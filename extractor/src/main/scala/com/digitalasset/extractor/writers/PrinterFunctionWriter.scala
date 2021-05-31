// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.writers

import com.daml.lf.iface.Interface
import com.daml.ledger.service.LedgerReader.PackageStore
import com.daml.extractor.ledger.types.{Event, TransactionTree}
import com.daml.extractor.writers.Writer.RefreshPackages

import scala.concurrent.Future
import scalaz.{\/, \/-}

trait PrinterFunctionWriter { self: Writer =>

  def printer: Any => Unit

  def handlePackage(id: String, interface: Interface): Unit

  def printEvent(event: Event): Unit

  def init(): Future[Unit] = {
    printer("==============")
    printer("Daml Extractor")
    printer("==============")

    Future.unit
  }

  def handlePackages(packageStore: PackageStore): Future[Unit] = {
    printer("====================")
    printer("Handling packages...")
    printer("====================")
    packageStore.foreach((handlePackage _).tupled)

    Future.unit
  }

  def handleTransaction(transaction: TransactionTree): Future[RefreshPackages \/ Unit] = {
    printer(s"Handling transaction #${transaction.transactionId}...")
    printer(s"Events:")
    transaction.events.map(_._2).foreach(printEvent)

    Future.successful(\/-(()))
  }

  def getLastOffset: Future[Option[String]] = Future.successful(None)
}

// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import io.grpc.Channel

import scala.concurrent.{ExecutionContext, Future}

class LedgerApiServices(channel: Channel, val ledgerId: String) {

  val activeContractsService = new ActiveContractsService(channel, ledgerId)
  val commandService = new CommandService(channel)
  val commandCompletionService = new CommandCompletionService(channel, ledgerId)
  val packageManagementService = new PackageManagementService(channel)
  val partyManagementService = new PartyManagementService(channel)
  val transactionService = new TransactionService(channel, ledgerId)

}

object LedgerApiServices {
  def forChannel(channel: Channel)(implicit ec: ExecutionContext): Future[LedgerApiServices] = {
    val ledgerIdentityService: LedgerIdentityService = new LedgerIdentityService(channel)
    ledgerIdentityService.fetchLedgerId().map(ledgerId => new LedgerApiServices(channel, ledgerId))
  }
}

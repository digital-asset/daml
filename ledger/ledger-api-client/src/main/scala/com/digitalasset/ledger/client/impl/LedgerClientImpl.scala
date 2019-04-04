// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.impl

import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.LedgerClientConfiguration
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.commands.{CommandClient, SynchronousCommandClient}
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.client.services.transactions.TransactionClient

private[client] class LedgerClientImpl(
    transactionService: TransactionService,
    activeContractsService: ActiveContractsService,
    commandSubmissionService: CommandSubmissionService,
    commandCompletionService: CommandCompletionService,
    commandService: CommandService,
    packageService: PackageService,
    ledgerClientConfiguration: LedgerClientConfiguration,
    override val ledgerId: String
)(implicit esf: ExecutionSequencerFactory)
    extends LedgerClient {

  override def activeContractSetClient: ActiveContractSetClient =
    new ActiveContractSetClient(ledgerId, activeContractsService)

  override def commandClient: CommandClient =
    new CommandClient(
      commandSubmissionService,
      commandCompletionService,
      ledgerId,
      ledgerClientConfiguration.applicationId,
      ledgerClientConfiguration.commandClient
    )

  override def commandServiceClient: SynchronousCommandClient =
    new SynchronousCommandClient(commandService)

  override def packageClient: PackageClient =
    new PackageClient(ledgerId, packageService)

  override def transactionClient: TransactionClient =
    new TransactionClient(ledgerId, transactionService)

}

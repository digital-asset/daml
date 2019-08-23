// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import io.grpc.Channel

private[infrastructure] final class LedgerServices(channel: Channel) {
  val activeContracts: ActiveContractsService = ActiveContractsServiceGrpc.stub(channel)
  val command: CommandService = CommandServiceGrpc.stub(channel)
  val identity: LedgerIdentityService = LedgerIdentityServiceGrpc.stub(channel)
  val partyManagement: PartyManagementService = PartyManagementServiceGrpc.stub(channel)
  val packageManagement: PackageManagementService = PackageManagementServiceGrpc.stub(channel)
  val transaction: TransactionService = TransactionServiceGrpc.stub(channel)
  val time: TimeService = TimeServiceGrpc.stub(channel)
}
